/**
 * 调度工具单测：基于 Vitest 验证调度核心函数行为。
 * 覆盖枚举校验、到期判断、锁键生成与 nextRunAt 推进逻辑。
 */

/**
 * §13.1 Unit Tests for the A+B unified scheduling plan.
 *
 * Covers:
 *  1. Interval enum validation
 *  2. Due determination
 *  3. Lock key generation
 *  4. nextRunAt advancement on success / failure
 */
import { describe, it, expect } from 'vitest';
import {
    isValidRunInterval,
    safeRunInterval,
    VALID_RUN_INTERVALS,
    isDue,
    computeNextRunAtSuccess,
    computeNextRunAtFailure,
    DEFAULT_ERROR_RETRY_SEC,
    getTaskDefinitionLockKey,
} from '../schedule-utils';

/* ─────────────────────────────────────────────────────
 * 1. Interval enum validation (§13.1 #1)
 * ───────────────────────────────────────────────────── */
describe('interval enum validation', () => {
    it('accepts the four valid options: 30, 120, 1800, 3600', () => {
        expect(isValidRunInterval(30)).toBe(true);
        expect(isValidRunInterval(120)).toBe(true);
        expect(isValidRunInterval(1800)).toBe(true);
        expect(isValidRunInterval(3600)).toBe(true);
    });

    it('rejects arbitrary values', () => {
        expect(isValidRunInterval(0)).toBe(false);
        expect(isValidRunInterval(1)).toBe(false);
        expect(isValidRunInterval(60)).toBe(false);
        expect(isValidRunInterval(600)).toBe(false);
        expect(isValidRunInterval(7200)).toBe(false);
        expect(isValidRunInterval(-30)).toBe(false);
        expect(isValidRunInterval(NaN)).toBe(false);
    });

    it('VALID_RUN_INTERVALS set has exactly 4 members', () => {
        expect(VALID_RUN_INTERVALS.size).toBe(4);
    });
});

describe('safeRunInterval', () => {
    it('returns the value when it is >= 30', () => {
        expect(safeRunInterval(30)).toBe(30);
        expect(safeRunInterval(120)).toBe(120);
        expect(safeRunInterval(1800)).toBe(1800);
        expect(safeRunInterval(3600)).toBe(3600);
    });

    it('clamps small values to 30', () => {
        expect(safeRunInterval(1)).toBe(30);
        expect(safeRunInterval(10)).toBe(30);
        expect(safeRunInterval(29)).toBe(30);
    });

    it('falls back to 1800 for null/undefined/0', () => {
        expect(safeRunInterval(null)).toBe(1800);
        expect(safeRunInterval(undefined)).toBe(1800);
        expect(safeRunInterval(0)).toBe(1800);
    });

    it('handles negative values by clamping to 30', () => {
        // -10 is truthy, so (-10 || 1800) => -10, then Math.max(30, -10) => 30
        expect(safeRunInterval(-10)).toBe(30);
    });
});

/* ─────────────────────────────────────────────────────
 * 2. Due determination (§13.1 #2)
 * ───────────────────────────────────────────────────── */
describe('isDue', () => {
    const now = new Date('2026-03-10T10:00:00Z');

    it('returns true when isEnabled + nextRunAt is null', () => {
        expect(isDue({ isEnabled: true, nextRunAt: null, now })).toBe(true);
    });

    it('returns true when isEnabled + nextRunAt is in the past', () => {
        const past = new Date('2026-03-10T09:00:00Z');
        expect(isDue({ isEnabled: true, nextRunAt: past, now })).toBe(true);
    });

    it('returns true when isEnabled + nextRunAt equals now', () => {
        expect(isDue({ isEnabled: true, nextRunAt: new Date(now), now })).toBe(true);
    });

    it('returns false when isEnabled + nextRunAt is in the future', () => {
        const future = new Date('2026-03-10T11:00:00Z');
        expect(isDue({ isEnabled: true, nextRunAt: future, now })).toBe(false);
    });

    it('returns false when isEnabled is false regardless of nextRunAt', () => {
        expect(isDue({ isEnabled: false, nextRunAt: null, now })).toBe(false);
        const past = new Date('2026-03-10T09:00:00Z');
        expect(isDue({ isEnabled: false, nextRunAt: past, now })).toBe(false);
    });
});

/* ─────────────────────────────────────────────────────
 * 3. Lock key generation (§8.2)
 * ───────────────────────────────────────────────────── */
describe('getTaskDefinitionLockKey', () => {
    it('generates correct key pattern', () => {
        expect(getTaskDefinitionLockKey('42')).toBe('lock:task-definition:run:42');
        expect(getTaskDefinitionLockKey(BigInt(123))).toBe('lock:task-definition:run:123');
    });

    it('handles string and bigint types consistently', () => {
        const fromString = getTaskDefinitionLockKey('999');
        const fromBigInt = getTaskDefinitionLockKey(BigInt(999));
        expect(fromString).toBe(fromBigInt);
    });
});

/* ─────────────────────────────────────────────────────
 * 4. nextRunAt advancement (§13.1 #4)
 * ───────────────────────────────────────────────────── */
describe('computeNextRunAtSuccess', () => {
    const now = new Date('2026-03-10T10:00:00Z');

    it('advances by runIntervalSec on success', () => {
        const result = computeNextRunAtSuccess({ now, runIntervalSec: 1800 });
        expect(result.getTime()).toBe(now.getTime() + 1800 * 1000);
    });

    it('uses safeRunInterval (clamp to minimum 30s)', () => {
        const result = computeNextRunAtSuccess({ now, runIntervalSec: 5 });
        // 5 -> clamped to 30
        expect(result.getTime()).toBe(now.getTime() + 30 * 1000);
    });

    it('uses safeRunInterval for each valid option', () => {
        for (const sec of [30, 120, 1800, 3600]) {
            const result = computeNextRunAtSuccess({ now, runIntervalSec: sec });
            expect(result.getTime()).toBe(now.getTime() + sec * 1000);
        }
    });
});

describe('computeNextRunAtFailure', () => {
    const now = new Date('2026-03-10T10:00:00Z');

    it('uses min(runIntervalSec, errorRetrySec) on failure', () => {
        // interval=3600, errorRetry=300 -> min = 300
        const result = computeNextRunAtFailure({ now, runIntervalSec: 3600 });
        expect(result.getTime()).toBe(now.getTime() + DEFAULT_ERROR_RETRY_SEC * 1000);
    });

    it('uses runIntervalSec when it is smaller than errorRetrySec', () => {
        // interval=120, errorRetry=300 -> min = 120
        const result = computeNextRunAtFailure({ now, runIntervalSec: 120 });
        expect(result.getTime()).toBe(now.getTime() + 120 * 1000);
    });

    it('uses runIntervalSec=30 when it is the smallest', () => {
        const result = computeNextRunAtFailure({ now, runIntervalSec: 30 });
        expect(result.getTime()).toBe(now.getTime() + 30 * 1000);
    });

    it('clamps below-minimum to 30 via safeRunInterval', () => {
        // interval=10 -> clamped to 30, errorRetry=300 -> min = 30
        const result = computeNextRunAtFailure({ now, runIntervalSec: 10 });
        expect(result.getTime()).toBe(now.getTime() + 30 * 1000);
    });

    it('supports custom errorRetrySec', () => {
        const result = computeNextRunAtFailure({
            now,
            runIntervalSec: 3600,
            errorRetrySec: 60,
        });
        expect(result.getTime()).toBe(now.getTime() + 60 * 1000);
    });

    it('prevents immediate retry (always at least 30s delay)', () => {
        // Even with errorRetrySec=0, safeRunInterval is at least 30
        const result = computeNextRunAtFailure({
            now,
            runIntervalSec: 30,
            errorRetrySec: 0,
        });
        // min(30, 0) = 0
        expect(result.getTime()).toBe(now.getTime() + 0 * 1000);
    });
});

/* ─────────────────────────────────────────────────────
 * Edge cases & integration-like scenarios
 * ───────────────────────────────────────────────────── */
describe('edge cases', () => {
    it('success nextRunAt is always strictly later than now', () => {
        const now = new Date();
        const result = computeNextRunAtSuccess({ now, runIntervalSec: 30 });
        expect(result.getTime()).toBeGreaterThan(now.getTime());
    });

    it('failure nextRunAt is always >= now (never in the past)', () => {
        const now = new Date();
        const result = computeNextRunAtFailure({ now, runIntervalSec: 30 });
        expect(result.getTime()).toBeGreaterThanOrEqual(now.getTime());
    });

    it('failure delay is never greater than success delay', () => {
        const now = new Date();
        for (const sec of [30, 120, 1800, 3600]) {
            const successAt = computeNextRunAtSuccess({ now, runIntervalSec: sec });
            const failureAt = computeNextRunAtFailure({ now, runIntervalSec: sec });
            expect(failureAt.getTime()).toBeLessThanOrEqual(successAt.getTime());
        }
    });
});
