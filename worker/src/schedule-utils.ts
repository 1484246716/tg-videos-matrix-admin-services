/**
 * Pure / side-effect-free utilities extracted from main.ts
 * for unit testing (§13 of the A+B plan).
 */

/* ── Interval validation ── */

export const VALID_RUN_INTERVALS = new Set([30, 120, 1800, 3600]);

/**
 * Returns true if the given value is one of the allowed runIntervalSec options.
 */
export function isValidRunInterval(value: number): boolean {
    return VALID_RUN_INTERVALS.has(value);
}

/**
 * Normalise raw interval value to a safe positive integer.
 * Mirrors the logic in scheduleEnabledTaskDefinitions().
 */
export function safeRunInterval(raw: number | null | undefined): number {
    return Math.max(30, raw || 1800);
}

/* ── Due determination ── */

/**
 * Returns true when a task definition should be executed in this tick.
 *
 * Conditions (per §7.1):
 *   - isEnabled = true
 *   - nextRunAt is null OR nextRunAt <= now
 */
export function isDue(args: {
    isEnabled: boolean;
    nextRunAt: Date | null;
    now?: Date;
}): boolean {
    if (!args.isEnabled) return false;
    const now = args.now ?? new Date();
    if (args.nextRunAt === null) return true;
    return args.nextRunAt.getTime() <= now.getTime();
}

/* ── nextRunAt advancement ── */

export const DEFAULT_ERROR_RETRY_SEC = 300;

/**
 * Compute the next run time after a **successful** execution.
 *
 * Formula: now + runIntervalSec
 */
export function computeNextRunAtSuccess(args: {
    now: Date;
    runIntervalSec: number;
}): Date {
    const safe = safeRunInterval(args.runIntervalSec);
    return new Date(args.now.getTime() + safe * 1000);
}

/**
 * Compute the next run time after a **failed** execution.
 *
 * Formula: now + min(runIntervalSec, errorRetrySec)
 * Prevents immediate storm-retry on failure.
 */
export function computeNextRunAtFailure(args: {
    now: Date;
    runIntervalSec: number;
    errorRetrySec?: number;
}): Date {
    const safe = safeRunInterval(args.runIntervalSec);
    const errorRetry = args.errorRetrySec ?? DEFAULT_ERROR_RETRY_SEC;
    const delaySec = Math.min(safe, errorRetry);
    return new Date(args.now.getTime() + delaySec * 1000);
}

/* ── Lock key ── */

/**
 * Generate the Redis lock key for a given task definition ID.
 */
export function getTaskDefinitionLockKey(taskDefinitionId: string | bigint): string {
    return `lock:task-definition:run:${taskDefinitionId.toString()}`;
}
