package io.confluent.ps.streams.referenceapp.chat.model;

import org.joda.time.DateTime;

import javax.annotation.Nonnull;

public class GoalEventWrapped extends GoalEvent {

  public static final int PROGRESSION_THRESHOLD = 3;

  public GoalEventWrapped() {
    super();
  }

  public GoalEventWrapped(@Nonnull GoalEvent w) {
    super(w.getId(), w.getTitle(), w.getScore(), w.getInitialScore(), w.getCreatedAt(), w.getServiceUserId(), w.getReviewedStatus(), w.getGoalProgressionSteps(), w.getSignificantProgressSurpassedAtStep());
  }

  public GoalEventWrapped(GoalId id, String title, Integer score, Integer initialScore, DateTime createdAt, ServiceUserId serviceUserId, ReviewedStatus reviewedStatus) {
    super(id, title, score, initialScore, createdAt, serviceUserId, reviewedStatus, 0, 0);
  }

  public boolean hasMadeProgress() {
    int scoreDiff = getProgress();
    boolean progress = scoreDiff > 0;
    return progress;
  }

  private int getProgress() {
    Integer currentScore = super.getScore();
    Integer initialScore = super.getInitialScore();
    return currentScore - initialScore;
  }

  public boolean hasProgressOfAtLeast(int progressAmountRequired) {
    int scoreDiff = getProgress();
    return (scoreDiff >= progressAmountRequired);
  }

  public boolean processStep() {
    Integer stepsSoFar = this.getGoalProgressionSteps();
    this.setGoalProgressionSteps(stepsSoFar + 1);

    boolean initialSignificantPassThisStep = this.hasSignificantProgress() && hasNotPreviouslyPassedSignificantThreshold();
    if (initialSignificantPassThisStep) {
      this.setSignificantProgressSurpassedAtStep(this.getGoalProgressionSteps());
    }

    return false;
  }

  public boolean isAtSignificantStep(){
    return this.getSignificantProgressSurpassedAtStep() == this.getGoalProgressionSteps();
  }

  private boolean hasNotPreviouslyPassedSignificantThreshold() {
    Integer passedAt = this.getSignificantProgressSurpassedAtStep();
    return passedAt == 0;
  }

  public boolean hasSignificantProgress() {
    return hasProgressOfAtLeast(PROGRESSION_THRESHOLD);
  }
}
