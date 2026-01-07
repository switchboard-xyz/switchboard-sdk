export enum TimeInterval {
  FIVE_MINUTES = 0,
  TEN_MINUTES = 1,
  FIFTEEN_MINUTES = 2,
  THIRTY_MINUTES = 3,
  ONE_HOUR = 4,
  TWO_HOURS = 5,
  SIX_HOURS = 6,
  TWELVE_HOURS = 7,
}

// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace TimeInterval {
  export function toString(interval?: TimeInterval): string {
    if (interval === undefined || interval === null) {
      return 'ONE_HOUR';
    }
    switch (interval) {
      case TimeInterval.FIVE_MINUTES:
        return 'FIVE_MINUTES';
      case TimeInterval.TEN_MINUTES:
        return 'TEN_MINUTES';
      case TimeInterval.FIFTEEN_MINUTES:
        return 'FIFTEEN_MINUTES';
      case TimeInterval.THIRTY_MINUTES:
        return 'THIRTY_MINUTES';
      case TimeInterval.ONE_HOUR:
        return 'ONE_HOUR';
      case TimeInterval.TWO_HOURS:
        return 'TWO_HOURS';
      case TimeInterval.SIX_HOURS:
        return 'SIX_HOURS';
      case TimeInterval.TWELVE_HOURS:
        return 'TWELVE_HOURS';
      default:
        return 'ONE_HOUR';
    }
  }

  export function fromString(interval: string): TimeInterval {
    if ((interval ?? '').length === 0) {
      return TimeInterval.ONE_HOUR;
    }
    switch (interval.toUpperCase()) {
      case 'FIVE_MINUTES':
        return TimeInterval.FIVE_MINUTES;
      case 'TEN_MINUTES':
        return TimeInterval.TEN_MINUTES;
      case 'FIFTEEN_MINUTES':
        return TimeInterval.FIFTEEN_MINUTES;
      case 'THIRTY_MINUTES':
        return TimeInterval.THIRTY_MINUTES;
      case 'ONE_HOUR':
        return TimeInterval.ONE_HOUR;
      case 'TWO_HOURS':
        return TimeInterval.TWO_HOURS;
      case 'SIX_HOURS':
        return TimeInterval.SIX_HOURS;
      case 'TWELVE_HOURS':
        return TimeInterval.TWELVE_HOURS;
      default:
        throw new Error(`Unknown time interval: ${interval}`);
    }
  }
}
