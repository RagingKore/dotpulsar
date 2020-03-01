/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DotPulsar.Internal
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;
    using PulsarApi;

    public sealed class ReaderChannelFactory : IReaderChannelFactory
    {
        readonly BatchHandler     _batchHandler;
        readonly IConnectionPool  _connectionPool;
        readonly Guid             _correlationId;
        readonly IRegisterEvent   _eventRegister;
        readonly IExecute         _executor;
        readonly uint             _messagePrefetchCount;
        readonly CommandSubscribe _subscribe;

        public ReaderChannelFactory(
            Guid correlationId,
            IRegisterEvent eventRegister,
            IConnectionPool connectionPool,
            IExecute executor,
            ReaderOptions options)
        {
            _correlationId        = correlationId;
            _eventRegister        = eventRegister;
            _connectionPool       = connectionPool;
            _executor             = executor;
            _messagePrefetchCount = options.MessagePrefetchCount;

            _subscribe = new CommandSubscribe
            {
                ConsumerName   = options.ReaderName,
                Durable        = false,
                ReadCompacted  = options.ReadCompacted,
                StartMessageId = options.StartMessageId.Data,
                Subscription   = $"Reader-{Guid.NewGuid():N}",
                Topic          = options.Topic
            };

            _batchHandler = new BatchHandler(false);
        }

        public async Task<IReaderChannel> Create(CancellationToken cancellationToken)
            => await _executor.Execute(() => GetChannel(cancellationToken), cancellationToken);

        async ValueTask<IReaderChannel> GetChannel(CancellationToken cancellationToken)
        {
            var connection   = await _connectionPool.FindConnectionForTopic(_subscribe.Topic, cancellationToken);
            var messageQueue = new AsyncQueue<MessagePackage>();
            var channel      = new Channel(_correlationId, _eventRegister, messageQueue);
            var response     = await connection.Send(_subscribe, channel);
            return new ConsumerChannel(response.ConsumerId, _messagePrefetchCount, messageQueue, connection, _batchHandler);
        }
    }
}