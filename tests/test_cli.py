from click.testing import CliRunner
from databusclient import cli


def test_mkdist_multiple_cv():
    runner = CliRunner()
    sha = 'a' * 64
    res = runner.invoke(cli.app, [
        'mkdist',
        'https://example.org/file',
        '--cv', 'b=2',
        '--cv', 'a=1',
        '--format', 'ttl',
        '--compression', 'gz',
        '--sha-length', f'{sha}:42'
    ])
    assert res.exit_code == 0, res.output
    # keys should be sorted alphabetically: a then b
    assert res.output.strip() == f'https://example.org/file|a=1_b=2|ttl|gz|{sha}:42'


def test_mkdist_invalid_cv():
    runner = CliRunner()
    res = runner.invoke(cli.app, ['mkdist', 'https://example.org/file', '--cv', 'badcv'])
    assert res.exit_code != 0
    assert 'Invalid content variant' in res.output


def test_mkdist_invalid_sha():
    runner = CliRunner()
    res = runner.invoke(cli.app, [
        'mkdist', 'https://example.org/file', '--cv', 'k=v', '--sha-length', 'abc:123'
    ])
    assert res.exit_code != 0
    assert 'Invalid --sha-length' in res.output


def test_completion_output():
    runner = CliRunner()
    res = runner.invoke(cli.app, ['completion', 'bash'])
    assert res.exit_code == 0
    assert '_DATABUSCLIENT_COMPLETE' in res.output
