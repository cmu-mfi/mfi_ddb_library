from typing import Optional
from configparser import ConfigParser
from pathlib import Path


def load_config(filename: Optional[str] = None, section: str = 'postgresql'):
    if filename is None:
        filename = str(Path(__file__).resolve().parents[1] / 'config' / 'pg_database.ini')

    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return config


if __name__ == '__main__':
    config = load_config()
    print(config)
