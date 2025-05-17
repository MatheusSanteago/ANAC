CREATE SCHEMA anac;

COMMENT ON SCHEMA anac is 'Dados do projeto ANAC';

CREATE TABLE IF NOT EXISTS anac.voos_nacionais (
    id SERIAL PRIMARY KEY,
    ano INTEGER,
    mes INTEGER,
    empresa TEXT,
    origem TEXT,
    destino TEXT,
    tarifa INTEGER,
    assentos INTEGER,
    ingestion_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS anac.voos_internacionais (
    id SERIAL PRIMARY KEY,
    ano INTEGER,
    mes INTEGER,
    empresa TEXT,
    origem TEXT,
    destino TEXT,
    retorno TEXT,
    classe_ida TEXT,
    classe_volta TEXT,
    tarifa INTEGER,
    assentos INTEGER,
    ingestion_time TIMESTAMP
);

-- Comentário para a tabela voos_nacionais
COMMENT ON TABLE anac.voos_nacionais IS 'Tabela com dados de voos nacionais da ANAC';

COMMENT ON COLUMN anac.voos_nacionais.id IS 'Identificador único do registro';
COMMENT ON COLUMN anac.voos_nacionais.ano IS 'Ano do voo';
COMMENT ON COLUMN anac.voos_nacionais.mes IS 'Mês do voo';
COMMENT ON COLUMN anac.voos_nacionais.empresa IS 'Nome da empresa aérea';
COMMENT ON COLUMN anac.voos_nacionais.origem IS 'Código do aeroporto de origem';
COMMENT ON COLUMN anac.voos_nacionais.destino IS 'Código do aeroporto de destino';
COMMENT ON COLUMN anac.voos_nacionais.tarifa IS 'Valor da tarifa em reais';
COMMENT ON COLUMN anac.voos_nacionais.assentos IS 'Número de assentos ofertados';
COMMENT ON COLUMN anac.voos_nacionais.ingestion_time IS 'Data e hora da ingestão dos dados';

-- Comentário para a tabela voos_internacionais
COMMENT ON TABLE anac.voos_internacionais IS 'Tabela com dados de voos internacionais da ANAC';

COMMENT ON COLUMN anac.voos_internacionais.id IS 'Identificador único do registro';
COMMENT ON COLUMN anac.voos_internacionais.ano IS 'Ano do voo';
COMMENT ON COLUMN anac.voos_internacionais.mes IS 'Mês do voo';
COMMENT ON COLUMN anac.voos_internacionais.empresa IS 'Nome da empresa aérea';
COMMENT ON COLUMN anac.voos_internacionais.origem IS 'Código do aeroporto de origem';
COMMENT ON COLUMN anac.voos_internacionais.destino IS 'Código do aeroporto de destino';
COMMENT ON COLUMN anac.voos_internacionais.retorno IS 'Indicador se o voo é de retorno (sim/não)';
COMMENT ON COLUMN anac.voos_internacionais.classe_ida IS 'Classe do voo de ida (ex: econômica, executiva)';
COMMENT ON COLUMN anac.voos_internacionais.classe_volta IS 'Classe do voo de volta (ex: econômica, executiva)';
COMMENT ON COLUMN anac.voos_internacionais.tarifa IS 'Valor da tarifa em reais';
COMMENT ON COLUMN anac.voos_internacionais.assentos IS 'Número de assentos ofertados';
COMMENT ON COLUMN anac.voos_internacionais.ingestion_time IS 'Data e hora da ingestão dos dados';
