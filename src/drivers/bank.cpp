#include "dataframe.h"
#include "datarepository.h"
#include "task.h"
#include "trigger.hpp"

#include <iostream>
#include <any>
#include <unordered_map>
#include <memory>
#include <mutex>

using DataFramePtr         = std::shared_ptr<DataFrame>;
using DataFrameWithIndexes = std::pair<std::vector<int>, DataFramePtr>;

class T1Transformer final : public Transformer {
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                   const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.size() < 2) return;
        auto inTrans = inputs[0].second;   // E1
        auto inUsers = inputs[1].second;   // E2
        auto out     = outputs[0];         // dfT1

        // posições E1
        int pTrId = inTrans->getColumn("id_transacao")         ->getPosition();
        int pUser = inTrans->getColumn("id_usuario_pagador")   ->getPosition();
        int pMod  = inTrans->getColumn("modalidade_pagamento") ->getPosition();
        int pVal  = inTrans->getColumn("valor_transacao")      ->getPosition();
        int pDate = inTrans->getColumn("data_horario")         ->getPosition();
        int pRegT = inTrans->getColumn("id_regiao")            ->getPosition();

        // posições E2 (agora incluindo limite_Boleto)
        int pKey    = inUsers->getColumn("id_usuario")           ->getPosition();
        int pSaldo  = inUsers->getColumn("saldo")                ->getPosition();
        int pPix    = inUsers->getColumn("limite_PIX")           ->getPosition();
        int pTed    = inUsers->getColumn("limite_TED")           ->getPosition();
        int pDoc    = inUsers->getColumn("limite_DOC")           ->getPosition();
        int pBol    = inUsers->getColumn("limite_Boleto")        ->getPosition();
        int pRegU   = inUsers->getColumn("id_regiao")            ->getPosition();

        // monta mapa: agora o tuple tem 5 limites + região
        std::unordered_map<std::string,
          std::tuple<double,double,double,double,double,std::string>
        > userMap;
        for (size_t r = 0; r < inUsers->size(); ++r) {
            auto uid  = inUsers->getElement<std::string>(r, pKey);
            auto sal  = inUsers->getElement<double>     (r, pSaldo);
            auto lpix = inUsers->getElement<double>     (r, pPix);
            auto lted = inUsers->getElement<double>     (r, pTed);
            auto ldoc = inUsers->getElement<double>     (r, pDoc);
            auto lbol = inUsers->getElement<double>     (r, pBol);
            auto regU = inUsers->getElement<std::string>(r, pRegU);
            userMap[uid] = std::make_tuple(sal, lpix, lted, ldoc, lbol, regU);
        }

        for (int idx : inputs[0].first) {
            // valores de E1
            auto trxId = inTrans->getElement<std::string>(idx, pTrId);
            auto usr   = inTrans->getElement<std::string>(idx, pUser);
            auto mod   = inTrans->getElement<std::string>(idx, pMod);
            auto val   = inTrans->getElement<double>     (idx, pVal);
            auto date  = inTrans->getElement<std::string>(idx, pDate);
            auto regT  = inTrans->getElement<std::string>(idx, pRegT);

            // desempacota info do usuário
            double sal, lpix, lted, ldoc, lbol;
            std::string regU;
            auto it = userMap.find(usr);
            if (it != userMap.end()) {
                std::tie(sal, lpix, lted, ldoc, lbol, regU) = it->second;
            }

            // monta row incluindo limite_Boleto (lbol)
            std::vector<std::any> row = {
                trxId,
                usr,
                mod,
                val,
                sal,
                lpix,
                lted,
                ldoc,
                lbol,
                date,
                regT,
                regU,
                static_cast<int>(true)
            };

            std::lock_guard<std::mutex> lk(writeMtx);
            out->addRow(row);
        }
    }
};


class T2Transformer final : public Transformer {
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                   const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.empty()) return;
        auto in  = inputs[0].second;   // df vindo de T1
        auto out = outputs[0];         // dfT2

        int pMod    = in->getColumn("modalidade_pagamento")->getPosition();
        int pVal    = in->getColumn("valor_transacao")     ->getPosition();
        int pSaldo  = in->getColumn("saldo")               ->getPosition();
        int pApr    = in->getColumn("aprovacao")           ->getPosition();

        for (int idx : inputs[0].first) {
            auto row = in->getRow(idx);

            if (row[pMod] != "CREDITO") {
                double valor = in->getElement<double>(idx, pVal);
                double saldo = in->getElement<double>(idx, pSaldo);
                if (saldo < valor) {
                    row[pApr] = std::to_string(0);
                }
            }

            {
                std::lock_guard<std::mutex> lk(writeMtx);
                out->addRow(row);
            }
        }
    }
};

class T6Transformer final : public Transformer {
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                   const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.empty()) return;
        auto in  = inputs[0].second;   // df de T1
        auto out = outputs[0];         // dfT6

        // posições em T1
        int pTrId = in->getColumn("id_transacao")    ->getPosition();
        int pUser = in->getColumn("id_usuario_pagador")->getPosition();
        int pVal  = in->getColumn("valor_transacao") ->getPosition();

        std::unordered_map<std::string, std::pair<double,int>> stats;
        for (size_t r = 0; r < in->size(); ++r) {
            auto uid = in->getElement<std::string>(r, pUser);
            double v = in->getElement<double>     (r, pVal);
            auto &pr = stats[uid];
            pr.first  += v;
            pr.second += 1;
        }
        std::unordered_map<std::string,double> avg;
        for (auto &kv : stats) {
            avg[kv.first] = kv.second.first / kv.second.second;
        }

        // 2) para cada transação, gera (id_tr, score)
        for (int idx : inputs[0].first) {
            auto trxId = in->getElement<std::string>(idx, pTrId);
            auto uid   = in->getElement<std::string>(idx, pUser);
            double v   = in->getElement<double>     (idx, pVal);
            double mean = avg[uid];

            // score: razão valor/mean (quanto maior, mais “arriscado”)
            double score = (mean > 0 ? v / mean : 0.0);

            // monta linha de saída: [id_transacao, score_risco]
            std::vector<std::string> row = {
                trxId,
                std::to_string(score)
            };

            std::lock_guard<std::mutex> lk(writeMtx);
            out->addRow(row);
        }
    }
};


class T7Transformer final : public Transformer {
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                   const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.empty()) return;
        auto in  = inputs[0].second;   // df de T1
        auto out = outputs[0];         // dfT7

        int pTrId  = in->getColumn("id_transacao") ->getPosition();
        int pDate  = in->getColumn("data_horario") ->getPosition();

        for (int idx : inputs[0].first) {
            const std::string ts = in->getElement<std::string>(idx, pDate);
            if (ts.size() < 13) continue;
            std::string hourStr = ts.substr(11, 2);
            if (!std::isdigit(hourStr[0]) || !std::isdigit(hourStr[1])) continue;
            int hour = std::stoi(hourStr);

            double score = std::abs(hour - 12) / 12.0;

            std::vector<std::string> row = {
                in->getElement<std::string>(idx, pTrId),
                std::to_string(score)
            };

            std::lock_guard<std::mutex> lk(writeMtx);
            out->addRow(row);
        }
    }
};

class T3Transformer final : public Transformer {
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                   const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.empty()) return;
        auto in  = inputs[0].second;   // df vindo de T2
        auto out = outputs[0];         // dfT3

        // posições das colunas em 'in'
        int pMod    = in->getColumn("modalidade_pagamento")->getPosition();
        int pVal    = in->getColumn("valor_transacao")     ->getPosition();
        int pPixLim = in->getColumn("limite_PIX")          ->getPosition();
        int pTedLim = in->getColumn("limite_TED")          ->getPosition();
        int pDocLim = in->getColumn("limite_DOC")          ->getPosition();
        int pBolLim = in->getColumn("limite_Boleto")       ->getPosition();
        int pApr    = in->getColumn("aprovacao")           ->getPosition();

        for (int idx : inputs[0].first) {
            auto row = in->getRow(idx);

            // só checa quem ainda está aprovado
            if (row[pApr] == "1") {
                double valor = in->getElement<double>(idx, pVal);
                double limite = 0.0;
                const auto &mod = row[pMod];

                if (mod == "PIX") {
                    limite = in->getElement<double>(idx, pPixLim);
                } else if (mod == "TED") {
                    limite = in->getElement<double>(idx, pTedLim);
                } else if (mod == "DOC") {
                    limite = in->getElement<double>(idx, pDocLim);
                } else if (mod == "Boleto") {
                     limite = in->getElement<double>(idx, pBolLim);
                }

                // se valor > limite, reprova
                if (valor > limite) {
                    row[pApr] = "0";
                }
            }

            // escreve na saída (mesmo formato, sem remover linhas)
            {
                std::lock_guard<std::mutex> lk(writeMtx);
                out->addRow(row);
            }
        }
    }
};


class PrintTransformer final : public Transformer {
private:
    std::string message;
public:
    PrintTransformer(std::string m = ""): message(m) {};
    void transform(std::vector<DataFramePtr>& outputs,
                   const std::vector<DataFrameWithIndexes>& inputs) override
    {
        std:: cout << message << std::endl;
        for(auto pair: inputs){
            std::cout << pair.second.get() << std::endl;
            std::cout << pair.second->toString() << std::endl;
        }
    }
};
void testePipelineTransacoes(int nThreads = 2) {

    //====================Construção dos DFS===========================//

    auto dfE1 = std::make_shared<DataFrame>();
    dfE1->addColumn<std::string>("id_transacao");
    dfE1->addColumn<std::string>("id_usuario_pagador");
    dfE1->addColumn<std::string>("id_usuario_recebedor");
    dfE1->addColumn<std::string>("id_regiao"); // ocorrência da região
    dfE1->addColumn<std::string>("modalidade_pagamento");
    dfE1->addColumn<std::string>("data_horario");
    dfE1->addColumn<double>     ("valor_transacao");

    auto dfE2 = std::make_shared<DataFrame>();
    dfE2->addColumn<std::string>("id_usuario");
    dfE2->addColumn<std::string>("id_regiao"); // região do usuário mora
    dfE2->addColumn<double>     ("saldo");
    dfE2->addColumn<double>     ("limite_PIX");
    dfE2->addColumn<double>     ("limite_TED");
    dfE2->addColumn<double>     ("limite_DOC");
    dfE2->addColumn<double>     ("limite_Boleto");

    auto dfT1 = std::make_shared<DataFrame>();
    dfT1->addColumn<std::string>("id_transacao");
    dfT1->addColumn<std::string>("id_usuario_pagador");
    dfT1->addColumn<std::string>("modalidade_pagamento");
    dfT1->addColumn<double>     ("valor_transacao");
    dfT1->addColumn<double>     ("saldo");
    dfT1->addColumn<double>     ("limite_PIX");
    dfT1->addColumn<double>     ("limite_TED");
    dfT1->addColumn<double>     ("limite_DOC");
    dfT1->addColumn<double>     ("limite_Boleto");
    dfT1->addColumn<std::string>("data_horario");
    dfT1->addColumn<std::string>("id_regiao_transacao");
    dfT1->addColumn<std::string>("id_regiao_usuario");
    dfT1->addColumn<int>        ("aprovacao");

    auto dfT2 = dfT1->emptyCopy();
    auto dfT3 = dfT2->emptyCopy();

    auto dfT6 = std::make_shared<DataFrame>();
    dfT6->addColumn<std::string>("id_transacao");
    dfT6->addColumn<double>     ("score_risco");

    auto dfT7 = std::make_shared<DataFrame>();
    dfT7->addColumn<std::string>("id_transacao");
    dfT7->addColumn<double>     ("score_risco");

     //====================Init Triggers===========================//

    auto e1 = std::make_shared<Extractor>();
    e1->addRepo(new FileRepository("data/transacoes_100k.csv", ",", true));
    e1->addOutput(dfE1);

    auto e2 = std::make_shared<Extractor>();
    e2->addRepo(new FileRepository("data/informacoes_cadastro_100k.csv", ",", true));
    e2->addOutput(dfE2);

    //==================== Construção dos DAG ===========================//
    auto t1 = std::make_shared<T1Transformer>();
    t1->addOutput(dfT1);

    auto tp1 = std::make_shared<PrintTransformer>(">>> T1 outputs");
    t1->addNext(tp1, {1});

    auto t2 = std::make_shared<T2Transformer>();
    t2->addOutput(dfT2);

    auto tp2 = std::make_shared<PrintTransformer>(">>> T2 outputs");
    t2->addNext(tp2, {1});

    auto t3 = std::make_shared<T3Transformer>();
    t3->addOutput(dfT3);


    auto tp3 = std::make_shared<PrintTransformer>(">>> T3 outputs");
    t3->addNext(tp3, {1});

    auto t6 = std::make_shared<T6Transformer>();
    t6->addOutput(dfT6);

    auto tp6 = std::make_shared<PrintTransformer>(">>> T6 outputs");
    t6->addNext(tp6, {1});

    auto t7 = std::make_shared<T7Transformer>();
    t7->addOutput(dfT7);

    auto tp7 = std::make_shared<PrintTransformer>(">>> T7 outputs");
    t7->addNext(tp7, {1});


    e1->addNext(t1, {1});
    e2->addNext(t1, {1});

    t1->addNext(t2, {1});
    t2->addNext(t3, {1});

    t1->addNext(t6, {1});
    t1->addNext(t7, {1});


    RequestTrigger trigger;
    trigger.addExtractor(e1);
    trigger.addExtractor(e2);
    trigger.start(nThreads);
}

int main() {
    testePipelineTransacoes();
    return 0;
}
