/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/**
 * Diagnostics, injectable. The library never writes to `console` on its
 * own; it reports through the {@link Logger} in its options, which
 * defaults to {@link noopLogger}. The Dart implementation logs under
 * `package:logging` names such as `Switchboard.Mux` and `Switchboard.Talk`;
 * the `name` argument carries the same names here so a consumer can route
 * them.
 */

/** Log severities, lowest first. */
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

/** Receives the library's diagnostics. */
export interface Logger {
  /**
   * @param level the severity
   * @param name the logger name, such as `Switchboard.Mux`
   * @param message what happened, already formatted
   * @param error the error involved, when there is one
   */
  log(level: LogLevel, name: string, message: string, error?: unknown): void;
}

/** Discards everything. The default. */
export const noopLogger: Logger = Object.freeze({
  log(): void {},
});

/** A {@link Logger} over `console`, for examples and debugging. */
export function consoleLogger(minimum: LogLevel = 'info'): Logger {
  const order: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 };
  const floor = order[minimum];
  return {
    log(level, name, message, error) {
      if (order[level] < floor) return;
      const line = `[${name}] ${message}`;
      if (error === undefined) console[level](line);
      else console[level](line, error);
    },
  };
}
