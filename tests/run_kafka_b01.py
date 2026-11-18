"""Run the built Kafka B01 target on a uniquely owned disposable Apache broker.

Build first (docs/KAFKA_FAILURE_ADMISSION.md). No shared broker/network/volume
mutations. The existing dependency-conan2 volume supplies compiled dependencies.
"""
import pathlib
import subprocess
import time
import uuid

root = pathlib.Path(__file__).resolve().parents[1]
identity = 'sa-coro-kafka-b01-' + uuid.uuid4().hex[:12]
broker = identity + '-broker'


def docker(*args, **kwargs):
    return subprocess.run(['docker', *args], check=True, timeout=30, **kwargs)


try:
    docker('run', '-d', '--name', broker, '--network', 'none', '--hostname', 'localhost',
           '--cpus=2', '--memory=1g',
           '-e', 'KAFKA_NODE_ID=1', '-e', 'KAFKA_PROCESS_ROLES=broker,controller',
           '-e', 'KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093',
           '-e', 'KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092',
           '-e', 'KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093',
           '-e', 'KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER',
           '-e', 'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
           '-e', 'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1',
           '-e', 'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0',
           '-e', 'KAFKA_HEAP_OPTS=-Xms256m -Xmx512m', 'apache/kafka:4.1.2')
    until = time.monotonic() + 75
    while True:
        result = subprocess.run(['docker', 'logs', broker], capture_output=True, timeout=5)
        if b'Kafka Server started' in result.stdout + result.stderr:
            break
        if time.monotonic() >= until:
            raise RuntimeError('test-owned broker did not become ready')
        time.sleep(1)
    print(f'Isolated fixture: {identity}', flush=True)
    result = subprocess.run(['docker', 'run', '--rm', '--name', identity + '-test',
        '--cpus=2', '--network', 'container:' + broker,
        '-e', 'SA_TEST_KAFKA_BROKERS=127.0.0.1:9092',
        '-v', f'{root}:/workspace', '-v', 'dependency-conan2:/conan',
        '-w', '/workspace', 'cppcoroservicelib-build:latest',
        'build/kafka-b01/cppcoroservicelib_kafka_endpoints_test',
        '--gtest_filter=KafkaHeaderCopy.*:KafkaFailureAdmission.*:KafkaCommitBoundary.*:KafkaDataSource.*:'
        'LibrdkafkaClients.*Checkpoint*:LibrdkafkaClients.*Rebalance*'], timeout=360)
    raise SystemExit(result.returncode)
finally:
    subprocess.run(['docker', 'rm', '-f', identity + '-test'], capture_output=True, timeout=30)
    with open('/private/tmp/' + identity + '.broker.log', 'w') as output:
        subprocess.run(['docker', 'logs', broker], stdout=output, stderr=subprocess.STDOUT, timeout=15)
    subprocess.run(['docker', 'rm', '-f', '-v', broker], timeout=30)
