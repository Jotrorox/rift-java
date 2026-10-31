package com.jotrorox.rift.decision;

import java.util.concurrent.CompletionStage;

/**
 * Decides logins or transfers for {@link com.jotrorox.rift.RiftConnection#decide}
 * or {@link com.jotrorox.rift.RiftClient#decide}.
 *
 * <p>Return quickly and complete the stage from your own executor, for example
 * a database pool. The handler runs on the subscription's delivery thread, so
 * blocking in it delays every later request on that subscription.
 *
 * <p>If the handler throws or the stage fails, no reply is sent and Rift
 * applies its {@code on_timeout} policy once its timeout expires. A reply that
 * arrives after Rift's timeout is dropped, and a timed-out request may still
 * have been processed: decisions must not have side effects.
 */
@FunctionalInterface
public interface DecisionHandler {
    CompletionStage<Decision> decide(DecisionRequest request) throws Exception;
}
