export class ReadBudgetExceededError extends Error {
  constructor() {
    super('Redis read budget is busy');
  }
}

export class ReadTimeoutError extends Error {
  constructor() {
    super('Redis read timed out');
  }
}

export class ReadBudget {
  private active = 0;

  constructor(
    readonly maxConcurrentReads = 4,
    readonly timeoutMs = 2_000,
  ) {
    if (
      !Number.isInteger(maxConcurrentReads) ||
      maxConcurrentReads < 1 ||
      maxConcurrentReads > 16
    ) {
      throw new Error('maxConcurrentReads must be between 1 and 16');
    }
    if (!Number.isInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 30_000) {
      throw new Error('timeoutMs must be between 100 and 30000');
    }
  }

  run<T>(operation: () => Promise<T>): Promise<T> {
    if (this.active >= this.maxConcurrentReads) throw new ReadBudgetExceededError();
    this.active++;
    let timer: NodeJS.Timeout | undefined;
    const work = Promise.resolve().then(operation);
    void work.finally(() => this.active--).catch(() => undefined);
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new ReadTimeoutError()), this.timeoutMs);
      timer.unref();
    });
    return Promise.race([work, deadline]).finally(() => {
      if (timer) clearTimeout(timer);
    });
  }
}
