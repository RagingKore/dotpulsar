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

    public sealed class ProducerChannelFactory : IProducerChannelFactory
    {
        readonly CommandProducer _commandProducer;
        readonly IConnectionPool _connectionPool;
        readonly Guid            _correlationId;
        readonly IRegisterEvent  _eventRegister;
        readonly IExecute        _executor;
        readonly SequenceId      _sequenceId;

        public ProducerChannelFactory(
            Guid correlationId,
            IRegisterEvent eventRegister,
            IConnectionPool connectionPool,
            IExecute executor,
            ProducerOptions options)
        {
            _correlationId  = correlationId;
            _eventRegister  = eventRegister;
            _connectionPool = connectionPool;
            _executor       = executor;
            _sequenceId     = new SequenceId(options.InitialSequenceId);

            _commandProducer = new CommandProducer
            {
                ProducerName = options.ProducerName,
                Topic        = options.Topic
            };
        }

        public async Task<IProducerChannel> Create(CancellationToken cancellationToken)
            => await _executor.Execute(() => GetChannel(cancellationToken), cancellationToken);

        async ValueTask<IProducerChannel> GetChannel(CancellationToken cancellationToken)
        {
            var connection = await _connectionPool.FindConnectionForTopic(_commandProducer.Topic, cancellationToken);
            var channel    = new Channel(_correlationId, _eventRegister, new AsyncQueue<MessagePackage>());
            var response   = await connection.Send(_commandProducer, channel);
            return new ProducerChannel(response.ProducerId, response.ProducerName, _sequenceId, connection);
        }
    }
}