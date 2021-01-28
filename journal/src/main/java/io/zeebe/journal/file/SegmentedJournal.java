/*
 * Copyright 2017-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.journal.file;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Sets;
import io.zeebe.journal.Journal;
import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;
import io.zeebe.journal.StorageException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A file based journal. The journal is split into multiple segments files. */
public class SegmentedJournal implements Journal {
  private static final int SEGMENT_BUFFER_FACTOR = 3;
  private final JournalMetrics journalMetrics;
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final String name;
  private final File directory;
  private final int maxSegmentSize;
  private final int maxEntrySize;
  private final NavigableMap<Long, JournalSegment> segments = new ConcurrentSkipListMap<>();
  private final Collection<SegmentedJournalReader> readers = Sets.newConcurrentHashSet();
  private volatile JournalSegment currentSegment;
  private volatile boolean open = true;
  private final long minFreeDiskSpace;
  private final JournalIndex journalIndex;
  private final SegmentedJournalWriter writer;

  public SegmentedJournal(
      final String name,
      final File directory,
      final int maxSegmentSize,
      final int maxEntrySize,
      final long minFreeSpace,
      final JournalIndex journalIndex) {
    this.name = checkNotNull(name, "name cannot be null");
    this.directory = checkNotNull(directory, "directory cannot be null");
    this.maxSegmentSize = maxSegmentSize;
    this.maxEntrySize = maxEntrySize;
    journalMetrics = new JournalMetrics(name);
    minFreeDiskSpace = minFreeSpace;
    this.journalIndex = journalIndex;
    open();
    writer = new SegmentedJournalWriter(this);
  }

  /**
   * Returns a new SegmentedJournal builder.
   *
   * @return A new Segmented journal builder.
   */
  public static SegmentedJournalBuilder builder() {
    return new SegmentedJournalBuilder();
  }

  @Override
  public JournalRecord append(final long asqn, final DirectBuffer data) {
    return writer.append(asqn, data);
  }

  @Override
  public void append(final JournalRecord record) {
    writer.append(record);
  }

  @Override
  public void deleteAfter(final long indexExclusive) {
    writer.deleteAfter(indexExclusive);
  }

  @Override
  public void deleteUntil(final long index) {
    final Map.Entry<Long, JournalSegment> segmentEntry = segments.floorEntry(index);
    if (segmentEntry != null) {
      final SortedMap<Long, JournalSegment> compactSegments =
          segments.headMap(segmentEntry.getValue().index());
      if (!compactSegments.isEmpty()) {
        log.debug("{} - Compacting {} segment(s)", name, compactSegments.size());
        for (final JournalSegment segment : compactSegments.values()) {
          log.trace("Deleting segment: {}", segment);
          segment.compactIndex(index);
          segment.close();
          segment.delete();
          journalMetrics.decSegmentCount();
        }
        compactSegments.clear();
      }
    }
    resetHead(getFirstSegment().index());
  }

  @Override
  public void reset(final long nextIndex) {
    writer.reset(nextIndex);
  }

  @Override
  public long getLastIndex() {
    return writer.getLastIndex();
  }

  @Override
  public long getFirstIndex() {
    if (!isEmpty()) {
      return segments.firstEntry().getValue().index();
    } else {
      return 0;
    }
  }

  @Override
  public boolean isEmpty() {
    return writer.getNextIndex() - getFirstSegment().index() == 0;
  }

  @Override
  public void flush() {
    writer.flush();
  }

  @Override
  public JournalReader openReader() {
    final SegmentedJournalReader reader = new SegmentedJournalReader(this);
    readers.add(reader);
    return reader;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    segments
        .values()
        .forEach(
            segment -> {
              log.debug("Closing segment: {}", segment);
              segment.close();
            });
    currentSegment = null;
    open = false;
  }

  /** Opens the segments. */
  private synchronized void open() {
    final long startTime = System.currentTimeMillis();
    // Load existing log segments from disk.
    for (final JournalSegment segment : loadSegments()) {
      segments.put(segment.descriptor().index(), segment);
      journalMetrics.incSegmentCount();
    }

    // If a segment doesn't already exist, create an initial segment starting at index 1.
    if (!segments.isEmpty()) {
      currentSegment = segments.lastEntry().getValue();
    } else {
      final JournalSegmentDescriptor descriptor =
          JournalSegmentDescriptor.builder()
              .withId(1)
              .withIndex(1)
              .withMaxSegmentSize(maxSegmentSize)
              .build();

      currentSegment = createSegment(descriptor);
      currentSegment.descriptor().update(System.currentTimeMillis());

      segments.put(1L, currentSegment);
      journalMetrics.incSegmentCount();
    }
    journalMetrics.observeJournalOpenDuration(System.currentTimeMillis() - startTime);
  }

  /**
   * Asserts that the journal is open.
   *
   * @throws IllegalStateException if the journal is not open
   */
  private void assertOpen() {
    checkState(currentSegment != null, "journal not open");
  }

  /** Asserts that enough disk space is available to allocate a new segment. */
  private void assertDiskSpace() {
    if (directory().getUsableSpace()
        < Math.max(maxSegmentSize() * SEGMENT_BUFFER_FACTOR, minFreeDiskSpace)) {
      throw new StorageException.OutOfDiskSpace(
          "Not enough space to allocate a new journal segment");
    }
  }

  private long maxSegmentSize() {
    return maxSegmentSize;
  }

  private File directory() {
    return directory;
  }

  /** Resets the current segment, creating a new segment if necessary. */
  private synchronized void resetCurrentSegment() {
    final JournalSegment lastSegment = getLastSegment();
    if (lastSegment != null) {
      currentSegment = lastSegment;
    } else {
      final JournalSegmentDescriptor descriptor =
          JournalSegmentDescriptor.builder()
              .withId(1)
              .withIndex(1)
              .withMaxSegmentSize(maxSegmentSize)
              .build();

      currentSegment = createSegment(descriptor);

      segments.put(1L, currentSegment);
      journalMetrics.incSegmentCount();
    }
  }

  /**
   * Resets and returns the first segment in the journal.
   *
   * @param index the starting index of the journal
   * @return the first segment
   */
  JournalSegment resetSegments(final long index) {
    assertOpen();

    for (final JournalSegment segment : segments.values()) {
      segment.close();
      segment.delete();
      journalMetrics.decSegmentCount();
    }
    segments.clear();

    final JournalSegmentDescriptor descriptor =
        JournalSegmentDescriptor.builder()
            .withId(1)
            .withIndex(index)
            .withMaxSegmentSize(maxSegmentSize)
            .build();
    currentSegment = createSegment(descriptor);
    segments.put(index, currentSegment);
    journalMetrics.incSegmentCount();
    return currentSegment;
  }

  /**
   * Returns the first segment in the log.
   *
   * @throws IllegalStateException if the segment manager is not open
   */
  JournalSegment getFirstSegment() {
    assertOpen();
    final Map.Entry<Long, JournalSegment> segment = segments.firstEntry();
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Returns the last segment in the log.
   *
   * @throws IllegalStateException if the segment manager is not open
   */
  JournalSegment getLastSegment() {
    assertOpen();
    final Map.Entry<Long, JournalSegment> segment = segments.lastEntry();
    return segment != null ? segment.getValue() : null;
  }

  /**
   * Creates and returns the next segment.
   *
   * @return The next segment.
   * @throws IllegalStateException if the segment manager is not open
   */
  synchronized JournalSegment getNextSegment() {
    assertOpen();
    assertDiskSpace();

    final JournalSegment lastSegment = getLastSegment();
    final JournalSegmentDescriptor descriptor =
        JournalSegmentDescriptor.builder()
            .withId(lastSegment != null ? lastSegment.descriptor().id() + 1 : 1)
            .withIndex(currentSegment.lastIndex() + 1)
            .withMaxSegmentSize(maxSegmentSize)
            .build();

    currentSegment = createSegment(descriptor);

    segments.put(descriptor.index(), currentSegment);
    journalMetrics.incSegmentCount();
    return currentSegment;
  }

  /**
   * Returns the segment following the segment with the given ID.
   *
   * @param index The segment index with which to look up the next segment.
   * @return The next segment for the given index.
   */
  JournalSegment getNextSegment(final long index) {
    final Map.Entry<Long, JournalSegment> nextSegment = segments.higherEntry(index);
    return nextSegment != null ? nextSegment.getValue() : null;
  }

  /**
   * Returns the segment for the given index.
   *
   * @param index The index for which to return the segment.
   * @throws IllegalStateException if the segment manager is not open
   */
  synchronized JournalSegment getSegment(final long index) {
    assertOpen();
    // Check if the current segment contains the given index first in order to prevent an
    // unnecessary map lookup.
    if (currentSegment != null && index > currentSegment.index()) {
      return currentSegment;
    }

    // If the index is in another segment, get the entry with the next lowest first index.
    final Map.Entry<Long, JournalSegment> segment = segments.floorEntry(index);
    if (segment != null) {
      return segment.getValue();
    }
    return getFirstSegment();
  }

  /**
   * Removes a segment.
   *
   * @param segment The segment to remove.
   */
  synchronized void removeSegment(final JournalSegment segment) {
    segments.remove(segment.index());
    journalMetrics.decSegmentCount();
    segment.close();
    segment.delete();
    resetCurrentSegment();
  }

  /** Creates a new segment. */
  JournalSegment createSegment(final JournalSegmentDescriptor descriptor) {
    final File segmentFile = JournalSegmentFile.createSegmentFile(name, directory, descriptor.id());

    final RandomAccessFile raf;
    final FileChannel channel;
    try {
      raf = new RandomAccessFile(segmentFile, "rw");
      raf.setLength(descriptor.maxSegmentSize());
      channel = raf.getChannel();
    } catch (final IOException e) {
      throw new StorageException(e);
    }

    final ByteBuffer buffer = ByteBuffer.allocate(JournalSegmentDescriptor.BYTES);
    descriptor.copyTo(buffer);
    buffer.flip();
    try {
      channel.write(buffer);
    } catch (final IOException e) {
      throw new StorageException(e);
    } finally {
      try {
        channel.close();
        raf.close();
      } catch (final IOException e) {
        log.warn("Unexpected IOException on closing", e);
      }
    }
    final JournalSegment segment = newSegment(new JournalSegmentFile(segmentFile), descriptor);
    log.debug("Created segment: {}", segment);
    return segment;
  }

  /**
   * Creates a new segment instance.
   *
   * @param segmentFile The segment file.
   * @param descriptor The segment descriptor.
   * @return The segment instance.
   */
  protected JournalSegment newSegment(
      final JournalSegmentFile segmentFile, final JournalSegmentDescriptor descriptor) {
    return new JournalSegment(segmentFile, descriptor, maxEntrySize, journalIndex);
  }

  /** Loads a segment. */
  private JournalSegment loadSegment(final long segmentId) {
    final File segmentFile = JournalSegmentFile.createSegmentFile(name, directory, segmentId);
    final ByteBuffer buffer = ByteBuffer.allocate(JournalSegmentDescriptor.BYTES);
    try (final FileChannel channel = openChannel(segmentFile)) {
      channel.read(buffer);
      buffer.flip();
      final JournalSegmentDescriptor descriptor = new JournalSegmentDescriptor(buffer);
      final JournalSegment segment = newSegment(new JournalSegmentFile(segmentFile), descriptor);
      log.debug("Loaded disk segment: {} ({})", descriptor.id(), segmentFile.getName());
      return segment;
    } catch (final IOException e) {
      throw new StorageException(e);
    }
  }

  private FileChannel openChannel(final File file) {
    try {
      return FileChannel.open(
          file.toPath(),
          StandardOpenOption.CREATE,
          StandardOpenOption.READ,
          StandardOpenOption.WRITE);
    } catch (final IOException e) {
      throw new StorageException(e);
    }
  }

  /**
   * Loads all segments from disk.
   *
   * @return A collection of segments for the log.
   */
  protected Collection<JournalSegment> loadSegments() {
    // Ensure log directories are created.
    directory.mkdirs();

    final TreeMap<Long, JournalSegment> segments = new TreeMap<>();

    // Iterate through all files in the log directory.
    for (final File file : directory.listFiles(File::isFile)) {

      // If the file looks like a segment file, attempt to load the segment.
      if (JournalSegmentFile.isSegmentFile(name, file)) {
        final JournalSegmentFile segmentFile = new JournalSegmentFile(file);
        final ByteBuffer buffer = ByteBuffer.allocate(JournalSegmentDescriptor.BYTES);
        try (final FileChannel channel = openChannel(file)) {
          channel.read(buffer);
          buffer.flip();
        } catch (final IOException e) {
          throw new StorageException(e);
        }

        final JournalSegmentDescriptor descriptor = new JournalSegmentDescriptor(buffer);

        // Load the segment.
        final JournalSegment segment = loadSegment(descriptor.id());

        // Add the segment to the segments list.
        log.debug(
            "Found segment: {} ({})", segment.descriptor().id(), segmentFile.file().getName());
        segments.put(segment.index(), segment);
      }
    }

    // Verify that all the segments in the log align with one another.
    JournalSegment previousSegment = null;
    boolean corrupted = false;
    final Iterator<Entry<Long, JournalSegment>> iterator = segments.entrySet().iterator();
    while (iterator.hasNext()) {
      final JournalSegment segment = iterator.next().getValue();
      if (previousSegment != null && previousSegment.lastIndex() != segment.index() - 1) {
        log.warn(
            "Journal is inconsistent. {} is not aligned with prior segment {}",
            segment.file().file(),
            previousSegment.file().file());
        corrupted = true;
      }
      if (corrupted) {
        segment.close();
        segment.delete();
        iterator.remove();
      }
      previousSegment = segment;
    }

    return segments.values();
  }

  public void closeReader(final SegmentedJournalReader segmentedJournalReader) {
    readers.remove(segmentedJournalReader);
  }

  /**
   * Resets journal readers to the given head.
   *
   * @param index The index at which to reset readers.
   */
  void resetHead(final long index) {
    for (final SegmentedJournalReader reader : readers) {
      if (reader.getNextIndex() <= index) {
        reader.seek(index);
      }
    }
  }

  /**
   * Resets journal readers to the given tail.
   *
   * @param index The index at which to reset readers.
   */
  void resetTail(final long index) {
    for (final SegmentedJournalReader reader : readers) {
      if (reader.getNextIndex() >= index) {
        reader.seek(index);
      } else {
        // This is not thread safe https://github.com/zeebe-io/zeebe/issues/4198
        reader.seek(reader.getNextIndex());
      }
    }
  }

  public JournalMetrics getJournalMetrics() {
    return journalMetrics;
  }

  public JournalIndex getJournalIndex() {
    return journalIndex;
  }
}
