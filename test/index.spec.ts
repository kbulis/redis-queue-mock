import * as chai from 'chai';
import * as mock from '../lib';

describe('initializes client and validates push and pop usage', () => {

  const pusher: mock.RedisClient = mock.createClient('mocks://mocked');
  const client: mock.RedisClient = mock.createClient('mocks://mocked');
  
  it('should receive pushed queue messages', (done) => {
    client.blpop('blocking-on-this-queue', 0, (err: any, item: string[] | null): void => {
      chai.expect(item)
        .to.be.not.null;

      if (item) {
        chai.expect(item)
          .to.have.property('length').equal(2);

        chai.expect(item[0])
          .to.equal('blocking-on-this-queue');

        chai.expect(item[1])
          .to.equal('a value');

        done();
      }
    });

    pusher.rpush('blocking-on-this-queue', 'a value');
  });

  after(() => {
    pusher.quit();
    client.quit();
  });

});
