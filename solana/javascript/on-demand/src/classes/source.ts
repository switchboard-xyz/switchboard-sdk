export enum Source {
  WEIGHTED = 0,
  BINANCE = 1,
  OKX = 2,
  BYBIT = 3,
  COINBASE = 4,
  BITGET = 5,
  AUTO = 6,
  PYTH = 7,
  TITAN = 8,
  GATE = 9,
}

// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace Source {
  export function toString(source?: Source): string {
    if (source === undefined || source === null) {
      return 'AUTO';
    }
    switch (source) {
      case Source.WEIGHTED:
        return 'WEIGHTED';
      case Source.BINANCE:
        return 'BINANCE';
      case Source.OKX:
        return 'OKX';
      case Source.BYBIT:
        return 'BYBIT';
      case Source.COINBASE:
        return 'COINBASE';
      case Source.BITGET:
        return 'BITGET';
      case Source.AUTO:
        return 'AUTO';
      case Source.PYTH:
        return 'PYTH';
      case Source.TITAN:
        return 'TITAN';
      case Source.GATE:
        return 'GATE';
      default:
        return 'UNKNOWN';
    }
  }

  export function fromString(source: string): Source {
    if ((source ?? '').length === 0) {
      return Source.WEIGHTED;
    }
    switch (source.toUpperCase()) {
      case 'WEIGHTED':
        return Source.WEIGHTED;
      case 'BINANCE':
        return Source.BINANCE;
      case 'OKX':
        return Source.OKX;
      case 'BYBIT':
        return Source.BYBIT;
      case 'COINBASE':
        return Source.COINBASE;
      case 'BITGET':
        return Source.BITGET;
      case 'AUTO':
        return Source.AUTO;
      case 'PYTH':
        return Source.PYTH;
      case 'TITAN':
        return Source.TITAN;
      case 'GATE':
      case 'GATE.IO':
        return Source.GATE;
      default:
        throw new Error(`Unknown source: ${source}`);
    }
  }
}
