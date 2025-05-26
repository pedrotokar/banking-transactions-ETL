# Banking-Transactions-ETL

*Desenvolvido por Anderson Falcão, Guilherme Castilho, Pedro
Tokar, Tomás Lira e Vitor do Nascimento.*

-------------------------------------------------------------------------------

## Diferenças entre entrega A1 e A2

Para ver as diferenças do repositório git entre as entregas, utilize:

```bash
$ git diff entrega-A1 entrega-RPC
```

## gRPC

### Cliente

Os arquivos e instruções para executar os clientes estão em `extras/grpc_client`.

### Servidor

Abaixo encontram-se instruções para executar a pipeline como um servidor gRPC.

**Importante:** para o funcionamento correto de servidor e cliente, o arquivo
mock usado para base de dados deve ser o mesmo. Para assegurar isso, basta executar
o gerador de dados e se certificar de que o **mesmo** arquivo `.db` gerado se
encontre na pasta `data` tanto na máquina do servidor quanto na máquina do cliente.

Para executar o servidor grpc, primeiro certifique-se de que o gRPC está instalado
em seu sistema, e se não estiver, faça da forma como preferir. Com o gRPC instalado,
execute os seguintes comandos, começando da raiz do repositório:

```bash
$ protoc -I=proto --cpp_out=proto --grpc_out=proto --plugin=protoc-gen-grpc=/usr/bin/grpc_cpp_plugin proto/transaction.proto
$ mkdir build
$ cd build
$ cmake ..
$ make
$ cd ..
```

Para executar o servidor então bastará usar o executável resultante:

```bash
$ ./build/transaction_server
```

É essencial que o executável seja executado da raiz do repositório, e não da
pasta `build`.

Observação: no primeiro comando, altere `/usr/bin/grpc_cpp_plugin` para o caminho
onde se encontra o compilador de proto da sua instalação do gRPC. Para sistemas
Arch Linux com gRPC instalado via pacman, essa é a localização.

-------------------------------------------------------------------------------

## Sobre o repositório

Esse repositório é destinado à primeira avaliação da matéria de Computação
Escalável, lecionada no 5º período da graduação de Ciência de Dados e
Inteligência Artificial da FGV-EMAp. O trabalho consiste em duas partes principais:

### Desenvolvimento de um micro-framework

Criar um micro-framework para apoiar o desenvolvimento de pipelines de
processamento de dados. O micro-framework deve otimizar o uso dos recursos do
sistema (principalmente das unidades de processamento) de forma que a execução
de pipelines seja eficiente.

O micro-framework deve apresentar abstrações para DataFrames (representam um
conjunto de dados), repositórios de dados (fornecem interfaces para ler e escrever
em diferentes armazenamento de dados), tratadores (representam etapas de
processamento da pipeline) e triggers (representam formas diferentes de acionar
a pipeline).

O micro-framework deve retirar do usuário a responsabilidade de usar recursos do
sistema como threads, sendo responsável por balancear de maneira inteligente
a execução das tarefas para diferentes unidades de processamento do computador.

### Criação de um projeto exemplo

Criar um projeto de exemplo demonstrando a utilização e corretude do micro-
framework. No caso de nosso grupo, o projeto de exemplo consiste em uma pipeline
que aprova ou desaprova transações bancárias de um banco fictício. Para isso,
são feitas verificações no horário das transações, nos limites do usuário do banco,
nos locais em que estão sendo feitas as transações e nas modalidades das transações.

## Rodando o sistema

Primeiramente, para obter a versão mais recente do programa, é necessário clonar
esse repositório:

```bash
$ git clone https://github.com/TomasLira/Banking-Transactions-ETL
cd Banking-Transactions-ETL
```

Nosso trabalho conta com o comando `make` para gerenciar as compilações. Atualmente,
a pipeline do banco depende de arquivos `.csv` e arquivos `.db`. Para gerar esses
arquivos, primeiro é necessário baixar as bibliotecas Python necessárias
(de preferência em um ambiente virtual):

```bash
$ pip install -r requirements.txt
```

Após baixá-las, basta usar o comando:

```bash
$ make csvgen
```

Se tudo ocorrer corretamente, após usar esse comando os arquivos `.csv` e `.db`
(banco de dados SQLite) necessários para a execução estarão disponíveis na pasta
data. Para executar a pipeline de exemplo e gerar os arquivos de output, é necessário
usar o comando:

```bash
$ make bank
```

Esse comando compila o código C++ e executa a pipeline de exemplo.

Caso queira apenas compilar o código, use o comando:

```bash
$ make buildbank
```

Após executar o comando pela primeira vez, para testar com números diferentes de threads,
basta usar o comando:

```bash
$./bankETL <num_threads>
```

onde `<num_threads>` é o número de threads que você deseja usar para executar a
pipeline. O número de threads padrão é 1.

Ex: `./bankETL 4` executa a pipeline com 4 threads.

Para ver as análises resultantes da pipeline, abra o arquivo `score_analysis.ipynb`.

## Organização do repositório

- `data`: contém os arquivos csv que são usados para a pipeline de exemplo, além
de suas saídas.

- `output`: contém os arquivos resultantes da pipeline.

- `extras`: contém o código python para a geração do csv para a pipeline de exemplo.

- `include`: contém os arquivos de header (`.h`), que exportam as funções e classes
definidas no projeto.

- `scripts`: contém scripts shell usados no processo de compilação.

- `src`: contém o código fonte do projeto.

## Sobre o balanceamento de carga da biblioteca.

Nossa biblioteca utiliza uma abordagem "estática" para determinação de como balancear
a pipeline para aproveitar melhor os recursos do computador do usuário. Ao declarar
as operações e blocos da pipeline, o usuário pode definir parâmetros, como complexidade
estimada, para informar a pipeline de como se comportam as operações.

Dessa forma, um algorítmo pode ser executado no DAG para determinar, de antemão,
uma distribuição de threads para cada etapa que espera-se minimizar o tempo total
de execução.
