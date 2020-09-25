/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processor.workflow.deployment.model.validation;

import io.zeebe.el.Expression;
import io.zeebe.el.ExpressionLanguage;
import io.zeebe.engine.processor.workflow.ExpressionProcessor;
import io.zeebe.model.bpmn.instance.CatchEvent;
import io.zeebe.model.bpmn.instance.Process;
import io.zeebe.model.bpmn.instance.StartEvent;
import io.zeebe.model.bpmn.instance.TimerEventDefinition;
import io.zeebe.model.bpmn.util.time.RepeatingInterval;
import java.util.function.Predicate;
import org.camunda.bpm.model.xml.validation.ModelElementValidator;
import org.camunda.bpm.model.xml.validation.ValidationResultCollector;

public class TimerCatchEventExpressionValidator implements ModelElementValidator<CatchEvent> {

  private static final long NO_VARIABLE_SCOPE = -1L;

  private final ExpressionLanguage expressionLanguage;
  private final ExpressionProcessor expressionProcessor;

  public TimerCatchEventExpressionValidator(
      final ExpressionLanguage expressionLanguage, final ExpressionProcessor expressionProcessor) {
    this.expressionLanguage = expressionLanguage;
    this.expressionProcessor = expressionProcessor;
  }

  @Override
  public Class<CatchEvent> getElementType() {
    return CatchEvent.class;
  }

  @Override
  public void validate(
      final CatchEvent element, final ValidationResultCollector validationResultCollector) {

    // verify static expressions only because other expression may requires a variable context
    // - except for process start events because they don't have any variable context
    final var isTimerStartEventOfProcess =
        element instanceof StartEvent && element.getScope() instanceof Process;
    final Predicate<Expression> expressionFilter =
        expression -> isTimerStartEventOfProcess || expression.isStatic();

    element.getEventDefinitions().stream()
        .filter(TimerEventDefinition.class::isInstance)
        .map(TimerEventDefinition.class::cast)
        .forEach(definition -> validation(definition, expressionFilter, validationResultCollector));
  }

  private void validation(
      final TimerEventDefinition timerEventDefinition,
      final Predicate<Expression> expressionFilter,
      final ValidationResultCollector validationResultCollector) {

    try {
      evaluateTimerExpression(timerEventDefinition, expressionFilter);
    } catch (final TimerValidationException e) {
      validationResultCollector.addError(0, e.getMessage());
    }
  }

  private void evaluateTimerExpression(
      final TimerEventDefinition timerEventDefinition,
      final Predicate<Expression> expressionFilter) {

    if (timerEventDefinition.getTimeDuration() != null) {
      final String duration = timerEventDefinition.getTimeDuration().getTextContent();
      final var expression = expressionLanguage.parseExpression(duration);

      if (expressionFilter.test(expression)) {
        wrapFailure(
            () -> expressionProcessor.evaluateIntervalExpression(expression, NO_VARIABLE_SCOPE),
            "duration");
      }

    } else if (timerEventDefinition.getTimeCycle() != null) {
      final String cycle = timerEventDefinition.getTimeCycle().getTextContent();
      final var expression = expressionLanguage.parseExpression(cycle);

      if (expressionFilter.test(expression)) {
        wrapFailure(
            () -> {
              final var interval =
                  expressionProcessor.evaluateStringExpression(expression, NO_VARIABLE_SCOPE);
              RepeatingInterval.parse(interval);
            },
            "cycle");
      }

    } else if (timerEventDefinition.getTimeDate() != null) {
      final String timeDate = timerEventDefinition.getTimeDate().getTextContent();
      final var expression = expressionLanguage.parseExpression(timeDate);

      if (expressionFilter.test(expression)) {
        wrapFailure(
            () -> expressionProcessor.evaluateDateTimeExpression(expression, NO_VARIABLE_SCOPE),
            "date");
      }
    }
  }

  private void wrapFailure(final Runnable evaluation, final String timerType) {
    try {
      evaluation.run();
    } catch (final Exception e) {
      throw new TimerValidationException(timerType, e);
    }
  }

  private static final class TimerValidationException extends RuntimeException {

    private TimerValidationException(final String timerType, final Throwable cause) {
      super(String.format("Invalid timer %s expression (%s)", timerType, cause.getMessage()));
    }
  }
}