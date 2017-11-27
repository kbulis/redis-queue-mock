/**
 * RedisClient
 * 
 * Client interface for interacting with an in-memory redis-like store of list
 * values. This is not intended to provide a full redis implementation for
 * unit testing, rather just what I needed (and maybe you too) to pull off a
 * simple queuing and alerting interface backed by redis.
 * 
 * Expected operations include the following:
 * 
 * (1) push new, single values onto the tail of a named list (rpush)
 * (2) blocking pop string values from a named list (blpop)
 * (3) list pop from a named list (lpop)
 * (4) quit the connection (quit)
 * 
 * The operations are simplified, but do conform to the redis client interface.
 * 
 */
export interface RedisClient {

  /**
   * Inserts the specified value at the tail of the list stored at key. If
   * key does not exist, it is created as empty list before performing the
   * push operation. 
   * 
   * @returns
   * Integer reply: The length of the list after the push operation.
   */
  rpush(queueKey: string, value: string): number;

  /**
   * BLPOP is a blocking list pop primitive. It is the blocking version of
   * LPOP because it blocks the connection when there are no elements to
   * pop from any of the given lists. An element is popped from the head
   * of the first list that is non-empty, with the given keys being checked
   * in the order that they are given.
   * 
   * @returns
   * Callback reply: A null multi-bulk when no element could be popped and the
   * timeout expired, or a two-element multi-bulk with the first element
   * being the name of the key where an element was popped and the second
   * element being the value of the popped element.
   */
  blpop(queueKey: string, timeoutInSecs: number, callback: (err: any, item: string[] | null) => void): void;
 
  /**
   * Removes and returns the first element of the list stored at key.
   * 
   * @returns
   * Callback reply: The value of the first element, or null when key
   * does not exist.
   */
  lpop(queueKey: string, callback: (err: any, item: string | null) => void): void;

  /**
   * Attaches listener to events emitted with one of three possible labels:
   * 'error', or 'ready'.
   * 
   * @returns
   * Nothing
   */
  on(event: string, listener: (...args: any[]) => void): void;

  /**
   * Ask the server to close the connection. The connection is closed as soon
   * as all pending replies have been written to the client.
   * 
   * @returns
   * Simple string reply: always "OK".
   */
  quit(): string;

}

/**
 * RedisQueueMockStored
 * 
 * ...
 * ...
 * ...
 * 
 */
class RedisQueueMockStored {

  static stores: RedisQueueMockStored[] = [];
  
  storing: any[] = [];
  connect: string = '';

  initializeClient(): RedisQueueMockClient {
    return new RedisQueueMockClient(this);
  }

  constructor(connect: string) {
    RedisQueueMockStored.stores.push(this);
    this.connect = connect;
  }

}

/**
 * RedisQueueMockClient
 * 
 * ...
 * ...
 * ...
 * 
 */
class RedisQueueMockClient implements RedisClient {

  private store: RedisQueueMockStored;
  private error: (err: any) => void;
  
  /**
   * Inserts the specified value at the tail of the list stored at key. If
   * key does not exist, it is created as empty list before performing the
   * push operation. 
   * 
   * @returns
   * Integer reply: The length of the list after the push operation.
   */
  rpush(queueKey: string, value: string): number {
    const index = this.store.storing.findIndex(list => list.key === queueKey);

    if (index !== -1) {
      if (this.store.storing[index].watch.length > 0) {
        this.store.storing[index].watch.shift()(null, [ queueKey, value ]);

        return this.store.storing[index].items.length + 1;
      }

      return this.store.storing[index].items.push(value);
    }

    this.store.storing.push({
      key: queueKey,
      watch: [
      ],
      items: [
        value
      ],
    });

    return 1;
  }

  /**
   * BLPOP is a blocking list pop primitive. It is the blocking version of
   * LPOP because it blocks the connection when there are no elements to
   * pop from any of the given lists. An element is popped from the head
   * of the first list that is non-empty, with the given keys being checked
   * in the order that they are given.
   * 
   * @returns
   * Callback reply: A null multi-bulk when no element could be popped and the
   * timeout expired, or a two-element multi-bulk with the first element
   * being the name of the key where an element was popped and the second
   * element being the value of the popped element.
   */
  blpop(queueKey: string, timeoutInSecs: number, callback: (err: any, item: string[] | null) => void): void {
    const index = this.store.storing.findIndex(list => list.key === queueKey);
    
    if (index !== -1) {
      if (this.store.storing[index].items.length > 0) {
        callback(null, [ queueKey, this.store.storing[index].items.shift() ]);

        return;
      }

      this.store.storing[index].watch.push(callback);

      return;
    }

    this.store.storing.push({
      key: queueKey,
      watch: [
        callback
      ],
      items: [
      ],
    });
  }

  /**
   * Removes and returns the first element of the list stored at key.
   * 
   * @returns
   * Callback reply: The value of the first element, or null when key
   * does not exist.
   */
  lpop(queueKey: string, callback: (err: any, item: string | null) => void): void {
    const index = this.store.storing.findIndex(list => list.key === queueKey);
    
    if (index !== -1) {
      if (this.store.storing[index].items.length > 0) {
        callback(null, this.store.storing[index].items.shift());

        return;
      }
    }

    callback(null, null);
  }

  /**
   * Attaches listener to events emitted with one of these possible labels:
   * 'error'. Only one listener maintained per label at a time.
   * 
   * @returns
   * Nothing
   */
  on(event: string, listener: (...args: any[]) => void): void {
    if (event === 'error') {
      this.error = listener;
    }
  }

  /**
   * Ask the server to close the connection. The connection is closed as soon
   * as all pending replies have been written to the client.
   * 
   * @returns
   * Simple string reply: always "OK".
   */
  quit(): string {
    return 'OK';
  }

  constructor(store: RedisQueueMockStored) {
    this.store = store;
  }

}

/**
 * Initializes new client to a mocked queue store segregated by the given
 * connection string. Connection strings must start with 'mocks' protocol.
 * 
 * @param connect
 * connection string
 * 
 * @returns
 * Initialized, mock queue client.
 */
export function createClient(connect: string): RedisClient {
  if (connect.startsWith('mocks://') === true) {
    for (const store of RedisQueueMockStored.stores) {
      if (store.connect === connect) {
        return store.initializeClient();
      }
    }

    return new RedisQueueMockStored(connect).initializeClient();
  }

  throw new Error('invalid mock queue protocol');
}
