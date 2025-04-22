# Banking-Transactions-ETL

*Desenvolvido por Anderson Falcão, Guilherme Castilho, Pedro
Tokar, Tomás Lira e Vitor do Nascimento.*

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
a pipeline do banco depende de um
arquivo csv. Para gerar esse arquivo, primeiro é necessário baixar as bibliotecas
python necessárias (de preferência em um ambiente virtual):

```bash
$ pip install -r requirements.txt
```

Após baixá-las, basta usar o comando:

```bash
$ make csvgen
```

Para executar a pipeline de exemplo e fazer outputs é necessário usar o comando:

```bash
$ make bank
```

Para ver as análises resultantes da pipeline, abra o arquivo `summary_score_analisys.ipynb`.

## Organização do repositório

- `data`: contém os arquivos csv que são usados para a pipeline de exemplo, além
de suas saídas.

- `output`: contém os arquivos resultantes da pipeline.

- `extras`: contém o código python para a geração do csv para a pipeline de exemplo.

- `include`: contém os arquivos de header (`.h`), que exportam as funções e classes
definidas no projeto.

- `scripts`: contém scripts shell usados no processo de compilação.

- `src`: contém o código fonte do projeto.
