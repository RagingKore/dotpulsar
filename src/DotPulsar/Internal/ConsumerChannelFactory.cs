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

    public sealed class ConsumerChannelFactory : IConsumerChannelFactory
    {
        readonly BatchHandler     _batchHandler;
        readonly IConnectionPool  _connectionPool;
        readonly Guid             _correlationId;
        readonly IRegisterEvent   _eventRegister;
        readonly IExecute         _executor;
        readonly uint             _messagePrefetchCount;
        readonly CommandSubscribe _subscribe;

        public ConsumerChannelFactory(
            Guid correlationId,
            IRegisterEvent eventRegister,
            IConnectionPool connectionPool,
            IExecute executor,
            ConsumerOptions options)
        {
            _correlationId        = correlationId;
            _eventRegister        = eventRegister;
            _connectionPool       = connectionPool;
            _executor             = executor;
            _messagePrefetchCount = options.MessagePrefetchCount;

            _subscribe = new CommandSubscribe
            {
                ConsumerName    = options.ConsumerName,
                initialPosition = (CommandSubscribe.InitialPosition) options.InitialPosition,
                PriorityLevel   = options.PriorityLevel,
                ReadCompacted   = options.ReadCompacted,
                Subscription    = options.SubscriptionName,
                Topic           = options.Topic,
                Type            = (CommandSubscribe.SubType) options.SubscriptionType
            };

            _batchHandler = new BatchHandler(true);
        }

        public async Task<IConsumerChannel> Create(CancellationToken cancellationToken)
            => await _executor.Execute(() => GetChannel(cancellationToken), cancellationToken);

        async ValueTask<IConsumerChannel> GetChannel(CancellationToken cancellationToken)
        {
            var connection   = await _connectionPool.FindConnectionForTopic(_subscribe.Topic, cancellationToken);
            var messageQueue = new AsyncQueue<MessagePackage>();
            var channel      = new Channel(_correlationId, _eventRegister, messageQueue);
            var response     = await connection.Send(_subscribe, channel);
            return new ConsumerChannel(response.ConsumerId, _messagePrefetchCount, messageQueue, connection, _batchHandler);
        }
    }
}