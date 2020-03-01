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
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Abstractions;
    using DotPulsar.Exceptions;
    using Extensions;
    using PulsarApi;

    public sealed class ConnectionPool : IConnectionPool
    {
        readonly CancellationTokenSource               _cancellationTokenSource;
        readonly Task                                  _closeInactiveConnections;
        readonly CommandConnect                        _commandConnect;
        readonly ConcurrentDictionary<Uri, Connection> _connections;
        readonly Connector                             _connector;
        readonly EncryptionPolicy                      _encryptionPolicy;
        readonly AsyncLock                             _lock;
        readonly Uri                                   _serviceUrl;

        public ConnectionPool(CommandConnect commandConnect, Uri serviceUrl, Connector connector, EncryptionPolicy encryptionPolicy)
        {
            _lock                    = new AsyncLock();
            _commandConnect          = commandConnect;
            _serviceUrl              = serviceUrl;
            _connector               = connector;
            _encryptionPolicy        = encryptionPolicy;
            _connections             = new ConcurrentDictionary<Uri, Connection>();
            _cancellationTokenSource = new CancellationTokenSource();

            _closeInactiveConnections = CloseInactiveConnections(
                TimeSpan.FromSeconds(60),
                _cancellationTokenSource.Token
            ); //TODO Get '60' from configuration
        }

        public async ValueTask DisposeAsync()
        {
            _cancellationTokenSource.Cancel();
            await _closeInactiveConnections;

            await _lock.DisposeAsync();

            foreach (var serviceUrl in _connections.Keys.ToArray()) await DisposeConnection(serviceUrl);
        }

        public async ValueTask<IConnection> FindConnectionForTopic(string topic, CancellationToken cancellationToken)
        {
            var lookup = new CommandLookupTopic
            {
                Topic         = topic,
                Authoritative = false
            };

            var serviceUrl = _serviceUrl;

            while (true)
            {
                var connection = await GetConnection(serviceUrl, cancellationToken);
                var response   = await connection.Send(lookup);
                response.Expect(BaseCommand.Type.LookupResponse);

                if (response.LookupTopicResponse.Response == CommandLookupTopicResponse.LookupType.Failed)
                    response.LookupTopicResponse.Throw();

                lookup.Authoritative = response.LookupTopicResponse.Authoritative;

                serviceUrl = new Uri(GetBrokerServiceUrl(response.LookupTopicResponse));

                if (response.LookupTopicResponse.Response == CommandLookupTopicResponse.LookupType.Redirect
                 || !response.LookupTopicResponse.Authoritative)
                    continue;

                if (_serviceUrl.IsLoopback
                ) // LookupType is 'Connect', ServiceUrl is local and response is authoritative. Assume the Pulsar server is a standalone docker.
                    return connection;

                return await GetConnection(serviceUrl, cancellationToken);
            }
        }

        string GetBrokerServiceUrl(CommandLookupTopicResponse response)
        {
            var hasBrokerServiceUrl    = !string.IsNullOrEmpty(response.BrokerServiceUrl);
            var hasBrokerServiceUrlTls = !string.IsNullOrEmpty(response.BrokerServiceUrlTls);

            switch (_encryptionPolicy)
            {
                case EncryptionPolicy.EnforceEncrypted:
                    if (!hasBrokerServiceUrlTls)
                        throw new ConnectionSecurityException(
                            "Cannot enforce encrypted connections. The lookup topic response from broker gave no secure alternative."
                        );

                    return response.BrokerServiceUrlTls;
                case EncryptionPolicy.EnforceUnencrypted:
                    if (!hasBrokerServiceUrl)
                        throw new ConnectionSecurityException(
                            "Cannot enforce unencrypted connections. The lookup topic response from broker gave no unsecure alternative."
                        );

                    return response.BrokerServiceUrl;
                case EncryptionPolicy.PreferEncrypted:
                    return hasBrokerServiceUrlTls ? response.BrokerServiceUrlTls : response.BrokerServiceUrl;
                default:
                    return hasBrokerServiceUrl ? response.BrokerServiceUrl : response.BrokerServiceUrlTls;
            }
        }

        async ValueTask<Connection> GetConnection(Uri serviceUrl, CancellationToken cancellationToken)
        {
            using (await _lock.Lock(cancellationToken))
            {
                if (_connections.TryGetValue(serviceUrl, out var connection))
                    return connection;

                return await EstablishNewConnection(serviceUrl);
            }
        }

        async Task<Connection> EstablishNewConnection(Uri serviceUrl)
        {
            var stream     = await _connector.Connect(serviceUrl);
            var connection = new Connection(new PulsarStream(stream));
            DotPulsarEventSource.Log.ConnectionCreated();
            _connections[serviceUrl] = connection;
            _                        = connection.ProcessIncommingFrames().ContinueWith(t => DisposeConnection(serviceUrl));
            var response = await connection.Send(_commandConnect);
            response.Expect(BaseCommand.Type.Connected);
            return connection;
        }

        async ValueTask DisposeConnection(Uri serviceUrl)
        {
            if (_connections.TryRemove(serviceUrl, out var connection))
            {
                await connection.DisposeAsync();
                DotPulsarEventSource.Log.ConnectionDisposed();
            }
        }

        async Task CloseInactiveConnections(TimeSpan interval, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
                try
                {
                    await Task.Delay(interval, cancellationToken);

                    using (await _lock.Lock(cancellationToken))
                    {
                        var serviceUrls = _connections.Keys;

                        foreach (var serviceUrl in serviceUrls)
                        {
                            var connection = _connections[serviceUrl];
                            if (connection is null)
                                continue;

                            if (!await connection.HasChannels())
                                await DisposeConnection(serviceUrl);
                        }
                    }
                }
                catch
                {
                    // TODO RK: Ignored and yet we should probably log it.
                }
        }
    }
}