-- Garante que estamos usando o catálogo correto (substitua pelo nome do seu catálogo real)
USE CATALOG workspace;

-- Criação do Schema para abrigar o Volume onde vamos fazer o upload do Excel
-- Será substituído por uma pasta na nuvem ao final do projeto
CREATE SCHEMA IF NOT EXISTS dados
  COMMENT 'Schema placeholder para a landing zone dos arquivos xlsx';

-- Criação do Volume (Onde vamos jogar o .xlsx manualmente ou via pipeline)
CREATE VOLUME IF NOT EXISTS dados.raw
  COMMENT 'Volume para ingestão de dados brutos';

-- ==========================================
-- Criação dos Schemas da Arquitetura Medallion
-- ==========================================
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Camada de dados brutos ingeridos no formato Delta.';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Camada de dados higienizados, tipados e com regras de negócio aplicadas.';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Camada de agregação final, Feature Stores e Data Marts para os modelos de ML.';
