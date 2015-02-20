# desafio

Sistema que sincronize dados entre Cassandra e ElasticSearch

## Instalacao

### Requisitos

* JDK 7 (versao onde foi testado)
* Lein - https://github.com/technomancy/leiningen
* Cassandra (localhost)
* Elastisearch (localhost)
* Jsvc (para rodar programa como daemon. Esse utilitario esta nos repositorios do Ubuntu e Debian)

Clone o programa do Github. 

Para obter as dependencias, execute:
```sh
$ lein deps
```

Para gerar um arquivo .jar com todas das dependencias do projeto, execute:

```sh
$ lein uberjar
```

## Usage

Rodar o programa pode ser realizado com:
```sh
$ java -jar desafio-0.1.0-standalone.jar 5000
```

O sistema pode ser executado como um daemon, para executa-lo dessa forma devemos digitar:

```sh
$ ./start.sh
```
Para terminar o daemon, execute:
```sh
$ ./stop.sh
```

## Testes

Os testes unitarios para o sistema pode ser executado com:
```sh
$ lein test
```

## Opcoes

O tempo entre sincronizacoes pode ser configurado. No exemplo abaixo:
```sh
$ java -jar desafio-0.1.0-standalone.jar 10000
```
o valor 10000 eh o numero de milisegundos entre as atualizacoes, no caso 10000 equivale a 10 segundos.

Para alterar esse valor no daemon o arquivo start.sh deve ser editado.


## License

Copyright © 2015 Trumae

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
