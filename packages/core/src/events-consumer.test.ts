import { withResolvers } from '@workflow/utils';
import type { Event } from '@workflow/world';
import { describe, expect, it, vi } from 'vitest';
import {
  DEFERRED_CHECK_DELAY_MS,
  EventConsumerResult,
  EventsConsumer,
} from './events-consumer.js';

// Helper function to create mock events
function createMockEvent(overrides: Partial<Event> = {}): Event {
  return {
    id: 'event-1',
    workflow_run_id: 'run-1',
    event_type: 'test-event',
    event_data: { value: 'test' },
    sequence_number: 1,
    created_at: new Date(),
    ...overrides,
  };
}

// Default options for tests that don't care about onUnconsumedEvent
const defaultOptions = {
  onUnconsumedEvent: vi.fn(),
  getPromiseQueue: () => Promise.resolve(),
};

// Helper function to wait for next tick
function waitForNextTick(): Promise<void> {
  return new Promise((resolve) => process.nextTick(resolve));
}

// Waits past the deferred unconsumed-event window so a check that was not
// cancelled has definitely fired.
function waitPastDeferredCheck(): Promise<void> {
  return new Promise((resolve) =>
    setTimeout(resolve, DEFERRED_CHECK_DELAY_MS * 2)
  );
}

// Unlike createMockEvent above, this builds the real `Event` shape, which the
// post-terminal skip needs: it reads `eventType` and `correlationId`.
let realEventCounter = 0;
function createRealEvent(
  eventType: string,
  correlationId: string | undefined,
  overrides: Partial<Event> = {}
): Event {
  realEventCounter++;
  return {
    eventId: `evnt_${realEventCounter}`,
    runId: 'wrun_test',
    eventType,
    correlationId,
    eventData: {},
    createdAt: new Date(),
    ...overrides,
  } as unknown as Event;
}

/**
 * A consumer for one entity: takes every event carrying `correlationId` and
 * deregisters once it has taken `terminalType`. This is the shape the runtime's
 * step/wait consumers have, and the reason a straggler for that id has no
 * callback left to claim it.
 */
function entityConsumer(correlationId: string, terminalType: string) {
  return vi.fn((event: Event | null) => {
    if (event === null || event.correlationId !== correlationId) {
      return EventConsumerResult.NotConsumed;
    }
    return event.eventType === terminalType
      ? EventConsumerResult.Finished
      : EventConsumerResult.Consumed;
  });
}

describe('EventsConsumer', () => {
  describe('constructor', () => {
    it('should initialize with provided events', () => {
      const events = [createMockEvent(), createMockEvent({ id: 'event-2' })];
      const consumer = new EventsConsumer(events, defaultOptions);

      expect(consumer.events).toEqual(events);
      expect(consumer.eventIndex).toBe(0);
      expect(consumer.callbacks).toEqual([]);
    });

    it('should initialize with empty events array', () => {
      const consumer = new EventsConsumer([], defaultOptions);

      expect(consumer.events).toEqual([]);
      expect(consumer.eventIndex).toBe(0);
      expect(consumer.callbacks).toEqual([]);
    });
  });

  describe('subscribe', () => {
    it('should add callback to callbacks array', () => {
      const consumer = new EventsConsumer([], defaultOptions);
      const callback = vi.fn();

      consumer.subscribe(callback);

      expect(consumer.callbacks).toContain(callback);
      expect(consumer.callbacks).toHaveLength(1);
    });

    it('should add multiple callbacks in order', () => {
      const consumer = new EventsConsumer([], defaultOptions);
      const callback1 = vi.fn();
      const callback2 = vi.fn();
      const callback3 = vi.fn();

      consumer.subscribe(callback1);
      consumer.subscribe(callback2);
      consumer.subscribe(callback3);

      expect(consumer.callbacks).toEqual([callback1, callback2, callback3]);
    });

    it('should automatically trigger consume on subscribe', async () => {
      const event = createMockEvent();
      const consumer = new EventsConsumer([event], defaultOptions);
      const callback = vi.fn().mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback);
      await waitForNextTick();

      expect(callback).toHaveBeenCalledWith(event);
      expect(callback).toHaveBeenCalledTimes(1);
    });
  });

  describe('consume (implicit)', () => {
    it('should call callbacks with current event', async () => {
      const event = createMockEvent();
      const consumer = new EventsConsumer([event], defaultOptions);
      const callback = vi.fn().mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback);
      await waitForNextTick();

      expect(callback).toHaveBeenCalledWith(event);
      expect(callback).toHaveBeenCalledTimes(1);
    });

    it('should call callbacks with null when no events exist', async () => {
      const consumer = new EventsConsumer([], defaultOptions);
      const callback = vi.fn().mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback);
      await waitForNextTick();

      expect(callback).toHaveBeenCalledWith(null);
    });

    it('should increment event index and remove callback when callback returns Finished', async () => {
      const event1 = createMockEvent({ id: 'event-1' });
      const event2 = createMockEvent({ id: 'event-2' });
      const consumer = new EventsConsumer([event1, event2], defaultOptions);
      const callback = vi.fn().mockReturnValue(EventConsumerResult.Finished);

      consumer.subscribe(callback);
      await waitForNextTick();

      expect(consumer.eventIndex).toBe(1);
      expect(consumer.callbacks).toHaveLength(0);
    });

    it('should not increment event index when callback returns false', async () => {
      const event = createMockEvent();
      const consumer = new EventsConsumer([event], defaultOptions);
      const callback = vi.fn().mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback);
      await waitForNextTick();

      expect(consumer.eventIndex).toBe(0);
      expect(consumer.callbacks).toContain(callback);
    });

    it('should process multiple callbacks until one returns true', async () => {
      const event = createMockEvent();
      const consumer = new EventsConsumer([event], defaultOptions);
      const callback1 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);
      const callback2 = vi.fn().mockReturnValue(EventConsumerResult.Finished);
      const callback3 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback1);
      consumer.subscribe(callback2);
      consumer.subscribe(callback3);
      await waitForNextTick();

      expect(callback1).toHaveBeenCalledWith(event);
      expect(callback2).toHaveBeenCalledWith(event);
      expect(callback3).toHaveBeenCalledWith(null);
      expect(consumer.eventIndex).toBe(1);
      expect(consumer.callbacks).toEqual([callback1, callback3]);
    });

    it('should process all callbacks when none return true and call onUnconsumedEvent', async () => {
      const event = createMockEvent();
      const unconsumedReceived = withResolvers<Event>();
      const consumer = new EventsConsumer([event], {
        onUnconsumedEvent: unconsumedReceived.resolve,
        getPromiseQueue: () => Promise.resolve(),
      });
      const callback1 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);
      const callback2 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);
      const callback3 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback1);
      consumer.subscribe(callback2);
      consumer.subscribe(callback3);
      await waitForNextTick();

      expect(callback1).toHaveBeenCalledWith(event);
      expect(callback2).toHaveBeenCalledWith(event);
      expect(callback3).toHaveBeenCalledWith(event);
      expect(consumer.eventIndex).toBe(0);
      expect(consumer.callbacks).toEqual([callback1, callback2, callback3]);

      const unconsumedEvent = await unconsumedReceived.promise;
      expect(unconsumedEvent).toEqual(event);
    });

    it('should recursively process next event when current event is consumed', async () => {
      const event1 = createMockEvent({ id: 'event-1', sequence_number: 1 });
      const event2 = createMockEvent({ id: 'event-2', sequence_number: 2 });
      const consumer = new EventsConsumer([event1, event2], defaultOptions);
      const callback1 = vi.fn().mockReturnValue(EventConsumerResult.Finished);
      const callback2 = vi.fn().mockReturnValue(EventConsumerResult.Finished);

      consumer.subscribe(callback1);
      consumer.subscribe(callback2);
      await waitForNextTick();
      await waitForNextTick(); // Wait for recursive processing

      expect(callback1).toHaveBeenCalledTimes(1);
      expect(callback1).toHaveBeenCalledWith(event1);
      expect(callback2).toHaveBeenCalledTimes(1);
      expect(callback2).toHaveBeenCalledWith(event2);
      expect(consumer.eventIndex).toBe(2);
      expect(consumer.callbacks).toHaveLength(0);
    });

    it('should drain consecutively consumable events within a single tick', async () => {
      // Optimization: when the consumers for a run of events are already
      // registered (the common replay case), the consumer drains them all in
      // one synchronous pass rather than paying one process.nextTick per
      // event. A single tick is enough to fully advance the index here.
      const events = [
        createMockEvent({ id: 'event-1', sequence_number: 1 }),
        createMockEvent({ id: 'event-2', sequence_number: 2 }),
        createMockEvent({ id: 'event-3', sequence_number: 3 }),
      ];
      const consumer = new EventsConsumer(events, defaultOptions);
      // A single long-lived consumer that consumes every real event (mirrors a
      // step consumer walking step_created -> step_started -> step_completed)
      // and returns NotConsumed once it reaches the end-of-events sentinel.
      const callback = vi
        .fn()
        .mockImplementation((event: Event | null) =>
          event === null
            ? EventConsumerResult.NotConsumed
            : EventConsumerResult.Consumed
        );

      consumer.subscribe(callback);
      await waitForNextTick();

      // Three real events consumed plus one call with the null sentinel, all
      // within a single tick.
      expect(callback).toHaveBeenCalledTimes(4);
      expect(callback).toHaveBeenNthCalledWith(1, events[0]);
      expect(callback).toHaveBeenNthCalledWith(2, events[1]);
      expect(callback).toHaveBeenNthCalledWith(3, events[2]);
      expect(callback).toHaveBeenNthCalledWith(4, null);
      expect(consumer.eventIndex).toBe(3);
    });

    it('should handle event index beyond events array length', async () => {
      const event = createMockEvent();
      const consumer = new EventsConsumer([event], defaultOptions);
      const callback = vi.fn().mockReturnValue(EventConsumerResult.Finished);

      consumer.subscribe(callback);
      await waitForNextTick();

      // Now eventIndex is 1, but array only has 1 element (index 0)
      const callback2 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);
      consumer.subscribe(callback2);
      await waitForNextTick();

      expect(callback2).toHaveBeenCalledWith(null);
    });

    it('should handle complex event processing scenario', async () => {
      const events = [
        createMockEvent({ id: 'event-1', event_type: 'type-a' }),
        createMockEvent({ id: 'event-2', event_type: 'type-b' }),
        createMockEvent({ id: 'event-3', event_type: 'type-a' }),
      ];
      const consumer = new EventsConsumer(events, defaultOptions);

      // Callback that only processes type-a events
      const typeACallback = vi
        .fn()
        .mockImplementation((event: Event | null) => {
          return event?.event_type === 'type-a'
            ? EventConsumerResult.Finished
            : EventConsumerResult.NotConsumed;
        });

      // Callback that only processes type-b events
      const typeBCallback = vi
        .fn()
        .mockImplementation((event: Event | null) => {
          return event?.event_type === 'type-b'
            ? EventConsumerResult.Finished
            : EventConsumerResult.NotConsumed;
        });

      consumer.subscribe(typeACallback);
      consumer.subscribe(typeBCallback);
      await waitForNextTick();
      await waitForNextTick(); // Wait for recursive processing
      await waitForNextTick(); // Wait for final processing

      // typeACallback processes event-1 and gets removed, so it won't process event-3
      expect(typeACallback).toHaveBeenCalledTimes(1); // Called for event-1 only
      expect(typeBCallback).toHaveBeenCalledTimes(1); // Called for event-2
      expect(consumer.eventIndex).toBe(2); // Only 2 events processed (event-3 remains)
      expect(consumer.callbacks).toHaveLength(0); // Both callbacks removed after consuming their events
    });
  });

  describe('edge cases', () => {
    it('should handle callback that throws error gracefully', async () => {
      const event = createMockEvent();
      const consumer = new EventsConsumer([event], defaultOptions);
      const throwingCallback = vi.fn().mockImplementation(() => {
        throw new Error('Callback error');
      });
      const normalCallback = vi
        .fn()
        .mockReturnValue(EventConsumerResult.Finished);

      consumer.subscribe(throwingCallback);
      consumer.subscribe(normalCallback);
      await waitForNextTick();

      // Error is caught and logged via eventsLogger, processing continues to next callback
      expect(throwingCallback).toHaveBeenCalledWith(event);
      expect(normalCallback).toHaveBeenCalledWith(event);
    });

    it('should continue processing when onConsumedEvent throws', async () => {
      const event1 = createMockEvent({ id: 'event-1' });
      const event2 = createMockEvent({ id: 'event-2' });
      const onConsumedEvent = vi
        .fn()
        .mockImplementationOnce(() => {
          throw new Error('Observer error');
        })
        .mockImplementation(() => undefined);
      const consumer = new EventsConsumer([event1, event2], {
        ...defaultOptions,
        onConsumedEvent,
      });
      const callback1 = vi.fn().mockReturnValue(EventConsumerResult.Finished);
      const callback2 = vi.fn().mockReturnValue(EventConsumerResult.Finished);

      consumer.subscribe(callback1);
      consumer.subscribe(callback2);
      await waitForNextTick();
      await waitForNextTick();

      expect(onConsumedEvent).toHaveBeenNthCalledWith(1, event1);
      expect(onConsumedEvent).toHaveBeenNthCalledWith(2, event2);
      expect(consumer.eventIndex).toBe(2);
    });

    it('should handle callback removal during iteration', async () => {
      const event = createMockEvent();
      const consumer = new EventsConsumer([event], defaultOptions);
      const callback1 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);
      const callback2 = vi.fn().mockReturnValue(EventConsumerResult.Finished);
      const callback3 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback1);
      consumer.subscribe(callback2);
      consumer.subscribe(callback3);
      await waitForNextTick();

      // callback2 should be removed when it returns true
      expect(consumer.callbacks).toEqual([callback1, callback3]);
      expect(callback3).toHaveBeenCalledWith(null);
    });

    it('should handle events with null/undefined data', async () => {
      const eventWithNullData = createMockEvent({ event_data: null as any });
      const consumer = new EventsConsumer([eventWithNullData], defaultOptions);
      const callback = vi.fn().mockReturnValue(EventConsumerResult.Finished);

      consumer.subscribe(callback);
      await waitForNextTick();

      expect(callback).toHaveBeenCalledWith(eventWithNullData);
      expect(consumer.eventIndex).toBe(1);
    });

    it('should handle multiple subscriptions happening in sequence', async () => {
      const event1 = createMockEvent({ id: 'event-1' });
      const event2 = createMockEvent({ id: 'event-2' });
      const consumer = new EventsConsumer([event1, event2], defaultOptions);

      const callback1 = vi.fn().mockReturnValue(EventConsumerResult.Finished);
      const callback2 = vi.fn().mockReturnValue(EventConsumerResult.Finished);

      consumer.subscribe(callback1);
      await waitForNextTick();

      consumer.subscribe(callback2);
      await waitForNextTick();

      expect(callback1).toHaveBeenCalledWith(event1);
      expect(callback2).toHaveBeenCalledWith(event2);
      expect(consumer.eventIndex).toBe(2);
    });

    it('should handle empty events array gracefully', async () => {
      const consumer = new EventsConsumer([], defaultOptions);
      const callback = vi.fn().mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback);
      await waitForNextTick();

      expect(callback).toHaveBeenCalledWith(null);
      expect(consumer.eventIndex).toBe(0);
    });
  });

  describe('onUnconsumedEvent', () => {
    it('should call onUnconsumedEvent when a non-null event is not consumed by any callback', async () => {
      const event = createMockEvent();
      const unconsumedReceived = withResolvers<Event>();
      const consumer = new EventsConsumer([event], {
        onUnconsumedEvent: unconsumedReceived.resolve,
        getPromiseQueue: () => Promise.resolve(),
      });
      const callback = vi.fn().mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback);

      const unconsumedEvent = await unconsumedReceived.promise;
      expect(unconsumedEvent).toEqual(event);
    });

    it('should NOT call onUnconsumedEvent for null event (end-of-events)', async () => {
      const onUnconsumedEvent = vi.fn();
      const consumer = new EventsConsumer([], {
        onUnconsumedEvent,
        getPromiseQueue: () => Promise.resolve(),
      });
      const callback = vi.fn().mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback);

      // Wait for the callback to be invoked with null (end-of-events)
      await vi.waitFor(() => {
        expect(callback).toHaveBeenCalledWith(null);
      });

      // null events should never trigger onUnconsumedEvent
      expect(onUnconsumedEvent).not.toHaveBeenCalled();
    });

    it('should cancel pending unconsumed check when a new callback subscribes', async () => {
      const event = createMockEvent();
      const onUnconsumedEvent = vi.fn();
      const consumer = new EventsConsumer([event], {
        onUnconsumedEvent,
        getPromiseQueue: () => Promise.resolve(),
      });
      const callback1 = vi
        .fn()
        .mockReturnValue(EventConsumerResult.NotConsumed);

      consumer.subscribe(callback1);
      await waitForNextTick();

      // Before the macrotask fires, subscribe a new callback that consumes the event
      const callback2 = vi.fn().mockReturnValue(EventConsumerResult.Finished);
      consumer.subscribe(callback2);

      // Wait for the new callback to consume the event
      await vi.waitFor(() => {
        expect(consumer.eventIndex).toBe(1);
      });

      // Wait past the internal 100ms unconsumed-event setTimeout window to
      // ensure the cancelled check truly does not fire.
      await new Promise((resolve) => setTimeout(resolve, 150));

      // The new callback consumed the event, so onUnconsumedEvent should NOT be called
      expect(onUnconsumedEvent).not.toHaveBeenCalled();
    });
  });

  describe('post-terminal events', () => {
    it('skips a step_started written after step_completed for the same correlation id', async () => {
      const corr = 'step_A';
      const events = [
        createRealEvent('step_created', corr),
        createRealEvent('step_started', corr),
        createRealEvent('step_completed', corr),
        // Written by a concurrent replay working from a prefix that predates
        // the completion, so it lands after it.
        createRealEvent('step_started', corr),
      ];
      const onUnconsumedEvent = vi.fn();
      const onPostTerminalEvent = vi.fn();
      const consumer = new EventsConsumer(events, {
        onUnconsumedEvent,
        onPostTerminalEvent,
        getPromiseQueue: () => Promise.resolve(),
      });

      consumer.subscribe(entityConsumer(corr, 'step_completed'));
      await waitPastDeferredCheck();

      expect(consumer.eventIndex).toBe(events.length);
      expect(onUnconsumedEvent).not.toHaveBeenCalled();
      expect(onPostTerminalEvent).toHaveBeenCalledTimes(1);
      expect(onPostTerminalEvent).toHaveBeenCalledWith(events[3]);
    });

    it('skips a duplicate wait_completed', async () => {
      const corr = 'wait_A';
      const events = [
        createRealEvent('wait_created', corr),
        createRealEvent('wait_completed', corr),
        createRealEvent('wait_completed', corr),
      ];
      const onUnconsumedEvent = vi.fn();
      const onPostTerminalEvent = vi.fn();
      const consumer = new EventsConsumer(events, {
        onUnconsumedEvent,
        onPostTerminalEvent,
        getPromiseQueue: () => Promise.resolve(),
      });

      consumer.subscribe(entityConsumer(corr, 'wait_completed'));
      await waitPastDeferredCheck();

      expect(consumer.eventIndex).toBe(events.length);
      expect(onUnconsumedEvent).not.toHaveBeenCalled();
      expect(onPostTerminalEvent).toHaveBeenCalledWith(events[2]);
    });

    it('still reports an unconsumed event whose correlation id never went terminal', async () => {
      const events = [
        createRealEvent('step_created', 'step_A'),
        createRealEvent('step_started', 'step_A'),
        createRealEvent('step_completed', 'step_A'),
        // A different entity that no callback ever claims.
        createRealEvent('wait_created', 'wait_B'),
      ];
      const onUnconsumedEvent = vi.fn();
      const onPostTerminalEvent = vi.fn();
      const consumer = new EventsConsumer(events, {
        onUnconsumedEvent,
        onPostTerminalEvent,
        getPromiseQueue: () => Promise.resolve(),
      });

      consumer.subscribe(entityConsumer('step_A', 'step_completed'));
      await waitPastDeferredCheck();

      expect(consumer.eventIndex).toBe(3);
      expect(onPostTerminalEvent).not.toHaveBeenCalled();
      expect(onUnconsumedEvent).toHaveBeenCalledWith(events[3]);
    });

    it('does not treat step_retrying or hook_received as terminal', async () => {
      // Neither event ends its entity: a retry writes another step_started, and
      // a hook keeps receiving until it is disposed. An unclaimed event after
      // either one is still divergence.
      const events = [
        createRealEvent('step_created', 'step_A'),
        createRealEvent('step_retrying', 'step_A'),
        createRealEvent('step_started', 'step_A'),
      ];
      const onUnconsumedEvent = vi.fn();
      const consumer = new EventsConsumer(events, {
        onUnconsumedEvent,
        getPromiseQueue: () => Promise.resolve(),
      });

      // Consumes the first two events, then deregisters, leaving the trailing
      // step_started unclaimed.
      consumer.subscribe(entityConsumer('step_A', 'step_retrying'));
      await waitPastDeferredCheck();

      expect(onUnconsumedEvent).toHaveBeenCalledWith(events[2]);
    });

    it('never takes an event a registered callback still wants', async () => {
      // A callback that claims post-terminal events wins: the skip is a
      // last resort, consulted only after every callback declined.
      const corr = 'hook_A';
      const events = [
        createRealEvent('hook_created', corr),
        createRealEvent('hook_disposed', corr),
        createRealEvent('hook_received', corr),
      ];
      const onUnconsumedEvent = vi.fn();
      const onPostTerminalEvent = vi.fn();
      const consumer = new EventsConsumer(events, {
        onUnconsumedEvent,
        onPostTerminalEvent,
        getPromiseQueue: () => Promise.resolve(),
      });

      const callback = vi.fn((event: Event | null) =>
        event === null
          ? EventConsumerResult.NotConsumed
          : EventConsumerResult.Consumed
      );
      consumer.subscribe(callback);
      await waitPastDeferredCheck();

      expect(consumer.eventIndex).toBe(events.length);
      expect(callback).toHaveBeenCalledWith(events[2]);
      expect(onPostTerminalEvent).not.toHaveBeenCalled();
      expect(onUnconsumedEvent).not.toHaveBeenCalled();
    });

    it('does not advance the deterministic clock for a skipped event', async () => {
      const corr = 'step_A';
      const events = [
        createRealEvent('step_created', corr),
        createRealEvent('step_completed', corr),
        createRealEvent('step_created', corr),
      ];
      const onConsumedEvent = vi.fn();
      const consumer = new EventsConsumer(events, {
        onConsumedEvent,
        onUnconsumedEvent: vi.fn(),
        getPromiseQueue: () => Promise.resolve(),
      });

      consumer.subscribe(entityConsumer(corr, 'step_completed'));
      await waitPastDeferredCheck();

      expect(consumer.eventIndex).toBe(events.length);
      // The workflow body never observed the straggler, so a log containing it
      // must produce the same timestamps as a log that does not.
      expect(onConsumedEvent).toHaveBeenCalledTimes(2);
      expect(onConsumedEvent).not.toHaveBeenCalledWith(events[2]);
    });
  });
});
