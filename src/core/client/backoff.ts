/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * The reconnect backoff schedule shared by `ReconnectingClient` and
 * `PeerSet`. Internal to the package; not exported from the barrel.
 */

/** The settings of a {@link Backoff}, in milliseconds. */
export interface BackoffSettings {
  /** Delay after the first failure. */
  readonly initial: number;
  /** Upper bound of the delay before jitter. */
  readonly max: number;
  /** Growth per consecutive failure. */
  readonly factor: number;
  /** Random spread of each delay, as a fraction of it. */
  readonly jitter: number;
}

/**
 * Exponential backoff with jitter: the delay after `n` consecutive
 * failures is `min(initial * factor^n, max)`, multiplied by a uniform
 * random factor in `[1 - jitter, 1 + jitter]` (so a delay may exceed
 * {@link max} by up to that fraction). Delays are milliseconds, rounded to
 * the microsecond as in Dart.
 */
export class Backoff {
  /** Delay after the first failure. */
  readonly initial: number;
  /** Upper bound of the delay before jitter. */
  readonly max: number;
  /** Growth per consecutive failure. */
  readonly factor: number;
  /** Random spread of each delay, as a fraction of it. */
  readonly jitter: number;
  readonly #random: () => number;

  /**
   * Creates the schedule; [random] (default `Math.random`) gives the
   * jitter. Throws `RangeError` like {@link check}.
   */
  constructor(settings: BackoffSettings, random: () => number = Math.random) {
    Backoff.check(settings);
    this.initial = settings.initial;
    this.max = settings.max;
    this.factor = settings.factor;
    this.jitter = settings.jitter;
    this.#random = random;
  }

  /**
   * Throws `RangeError` (naming the options `initialBackoff`, `maxBackoff`,
   * `backoffFactor` and `jitter`, as the public constructors call them) if
   * the initial delay is negative or not finite, the maximum is below it,
   * the factor is below 1 or not finite, or the jitter is outside 0 to 1.
   */
  static check(settings: BackoffSettings): void {
    const { initial, max, factor, jitter } = settings;
    if (!(initial >= 0) || !Number.isFinite(initial)) {
      throw new RangeError(`initialBackoff ${initial} must not be negative`);
    }
    if (!(max >= initial)) {
      throw new RangeError(`maxBackoff ${max} must not be below initialBackoff ${initial}`);
    }
    if (!(factor >= 1) || !Number.isFinite(factor)) {
      throw new RangeError(`backoffFactor ${factor} must be a finite number of at least 1`);
    }
    if (!(jitter >= 0 && jitter <= 1)) {
      throw new RangeError(`jitter ${jitter} must be between 0 and 1`);
    }
  }

  /** The delay after [failures] consecutive failures, with jitter, in milliseconds. */
  delay(failures: number): number {
    if (this.initial === 0) {
      return 0;
    }
    let base = this.initial * this.factor ** failures;
    if (!(base <= this.max)) {
      base = this.max;
    }
    if (this.jitter > 0) {
      base *= 1 + this.jitter * (2 * this.#random() - 1);
    }
    return Math.round(base * 1000) / 1000;
  }
}
