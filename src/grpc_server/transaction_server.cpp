// ===========Versão com threadpool gerenciada pelog RPC===============
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "transaction.pb.h"
#include "transaction.grpc.pb.h"

#include "trigger.hpp"
#include "types.h"
#include "dataframe.h"
#include "task.h"
#include "pipelinemanager.hpp"
#include "datarepository.h"

using DataFramePtr         = std::shared_ptr<DataFrame>;
using DataFrameWithIndexes = std::pair<std::vector<int>, DataFramePtr>;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;

using transaction::Transaction;
using transaction::Result;
using transaction::TransactionService;


class T1Transformer final : public Transformer {
private:
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.size() < 2) return;
        auto inTrans = inputs[0].second;   // E1
        auto inUsers = inputs[1].second;   // E2
        auto out     = outputs[0];         // dfT1ndl;
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
        int pCre    = inUsers->getColumn("limite_CREDITO")       ->getPosition();
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
            auto lcre = inUsers->getElement<double>     (r, pCre);
            auto lbol = inUsers->getElement<double>     (r, pBol);
            auto regU = inUsers->getElement<std::string>(r, pRegU);
            userMap[uid] = std::make_tuple(sal, lpix, lted, lcre, lbol, regU);
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
            double sal, lpix, lted, lcre, lbol;
            std::string regU;
            auto it = userMap.find(usr);
            if (it != userMap.end()) {
                std::tie(sal, lpix, lted, lcre, lbol, regU) = it->second;
            }

            // monta row incluindo limite_Boleto (lbol)
            VarRow row = {
                trxId,
                usr,
                mod,
                val,
                sal,
                lpix,
                lted,
                lcre,
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
private:
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

class T3Transformer final : public Transformer {
private:
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
        int pCreLim = in->getColumn("limite_CREDITO")      ->getPosition();
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
                } else if (mod == "Boleto") {
                    limite = in->getElement<double>(idx, pBolLim);
                } else {
                    limite = in->getElement<double>(idx, pCreLim);
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

class T4Transformer final : public Transformer {
private:
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.size() < 2) return;
        // inputs[0]: extrator E3, inputs[1]: tratador T1
        auto dfReg = inputs[0].second;   // E3: regiões
        auto dfT1  = inputs[1].second;   // T1: transações enriquecidas
        auto out   = outputs[0];         // dfT4

        // --- monta mapa de coordenadas de região (id_regiao → (lat, lon)) ---
        int pR     = dfReg->getColumn("id_regiao") ->getPosition();
        int pLat   = dfReg->getColumn("latitude")  ->getPosition();
        int pLon   = dfReg->getColumn("longitude") ->getPosition();

        std::unordered_map<std::string, std::pair<double,double>> coordMap;
        for (size_t r = 0; r < dfReg->size(); ++r) {
            auto rid = dfReg->getElement<std::string>(r, pR);
            auto lat = dfReg->getElement<double>     (r, pLat);
            auto lon = dfReg->getElement<double>     (r, pLon);
            coordMap[rid] = { lat, lon };
        }

        // --- posições em dfT1 para id, regiões de transação e usuário ---
        int pTrId = dfT1->getColumn("id_transacao")       ->getPosition();
        int pRegT = dfT1->getColumn("id_regiao_transacao")->getPosition();
        int pRegU = dfT1->getColumn("id_regiao_usuario")  ->getPosition();

        // --- para cada índice autorizado, cria a linha de saída ---
        for (int idx : inputs[1].first) {
            auto trxId = dfT1->getElement<std::string>(idx, pTrId);

            std::string regT = dfT1->getElement<std::string>(idx, pRegT);
            std::string regU = dfT1->getElement<std::string>(idx, pRegU);

            double latT = 0, lonT = 0, latU = 0, lonU = 0;
            if (auto it = coordMap.find(regT); it != coordMap.end()) {
                latT = it->second.first;
                lonT = it->second.second;
            }
            if (auto it = coordMap.find(regU); it != coordMap.end()) {
                latU = it->second.first;
                lonU = it->second.second;
            }

            std::vector<std::any> row = {
                trxId,
                regT,
                latT,
                lonT,
                regU,
                latU,
                lonU
            };

            std::lock_guard<std::mutex> lk(writeMtx);
            out->addRow(row);
        }
    }
};

class T5Transformer final : public Transformer {
private:
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.empty()) return;
        auto dfT4 = inputs[0].second;   // saída de T4
        auto out  = outputs[0];         // dfT5

        int pTrId  = dfT4->getColumn("id_transacao")        ->getPosition();
        int pLatT  = dfT4->getColumn("latitude_transacao")  ->getPosition();
        int pLonT  = dfT4->getColumn("longitude_transacao") ->getPosition();
        int pLatU  = dfT4->getColumn("latitude_usuario")    ->getPosition();
        int pLonU  = dfT4->getColumn("longitude_usuario")   ->getPosition();

        for (int idx : inputs[0].first) {
            auto trxId = dfT4->getElement<std::string>(idx, pTrId);
            double latT = dfT4->getElement<double>(idx, pLatT);
            double lonT = dfT4->getElement<double>(idx, pLonT);
            double latU = dfT4->getElement<double>(idx, pLatU);
            double lonU = dfT4->getElement<double>(idx, pLonU);

            double dlat = latT - latU;
            double dlon = lonT - lonU;
            double score = std::sqrt(dlat*dlat + dlon*dlon);

            // monta a linha de saída
            std::vector<std::string> row = {
                trxId,
                std::to_string(score)
            };

            std::lock_guard<std::mutex> lk(writeMtx);
            out->addRow(row);
        }
    }
};

class T6Transformer final : public Transformer {
private:
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.empty()) return;
        auto in  = inputs[0].second;   // df de T1
        auto out = outputs[0];         // dfT6

        // posições em T1
        int pTrId = in->getColumn("id_transacao")      ->getPosition();
        int pUser = in->getColumn("id_usuario_pagador")->getPosition();
        int pVal  = in->getColumn("valor_transacao")   ->getPosition();

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
private:
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

class T8Transformer final : public Transformer {
    std::mutex writeMtx1;
    std::mutex writeMtx2;
    std::mutex writeMtx3;
    std::mutex writeMtx4;

public:
    // inputs:
    //   [0] = T6 (score_valor)
    //   [1] = T7 (score_horario)
    //   [2] = T5 (score_regiao)
    // outputs:
    //   [0] = main: (id_transacao, score_valor, score_horario, score_regiao, aprovacao)
    //   [1] = valor: (score_valor, aprovacao)
    //   [2] = horario: (score_horario, aprovacao)
    //   [3] = regiao: (score_regiao, aprovacao)
    void transform(std::vector<DataFramePtr>& outputs,
                const std::vector<DataFrameWithIndexes>& inputs) override
    {
        if (inputs.size() < 3) return;
        auto dfVal = inputs[0].second;
        auto dfHor = inputs[1].second;
        auto dfReg = inputs[2].second;

        // 1) calcula mediana dos somatórios
        std::vector<double> totals;
        totals.reserve(dfVal->size());
        int pScoreV = dfVal->getColumn("score_risco")->getPosition();
        int pScoreH = dfHor->getColumn("score_risco")->getPosition();
        int pScoreR = dfReg->getColumn("score_risco")->getPosition();
        for (size_t i = 0; i < dfVal->size(); ++i) {
            totals.push_back(
                dfVal->getElement<double>(i, pScoreV)
            + dfHor->getElement<double>(i, pScoreH)
            + dfReg->getElement<double>(i, pScoreR)
            );
        }
        std::sort(totals.begin(), totals.end());
        double tau = 0.0;
        size_t n = totals.size();
        if (n > 0) {
            tau = (n % 2 == 1)
                ? totals[n/2]
                : (totals[n/2 - 1] + totals[n/2]) / 2.0;
        }
        //tau = 0;
        // 2) posições e ponteiros de saída
        int pId      = dfVal->getColumn("id_transacao")->getPosition();
        auto outMain = outputs[0];
        auto outVal  = outputs[1];
        auto outHor  = outputs[2];
        auto outReg  = outputs[3];

        for (int idx : inputs[0].first) {
            auto trxId   = dfVal->getElement<std::string>(idx, pId);
            double sV    = dfVal->getElement<double>(idx, pScoreV);
            double sH    = dfHor->getElement<double>(idx, pScoreH);
            double sR    = dfReg->getElement<double>(idx, pScoreR);
            double total = sV + sH + sR;
            int aprov    = (total > tau*0.7) ? 1 : 0;

            {
                std::vector<std::any> row = { trxId, sV, sH, sR, aprov };
                std::lock_guard<std::mutex> lk1(writeMtx1);
                outMain->addRow(row);
            }
            // — L3:  score_valor + aprovação
            {
                std::vector<std::any> row = { sV, aprov };
                std::lock_guard<std::mutex> lk2(writeMtx2);
                outVal->addRow(row);
            }
            // — L4:  score_horario + aprovação
            {
                std::vector<std::any> row = { sH, aprov };
                std::lock_guard<std::mutex> lk3(writeMtx3);
                outHor->addRow(row);
            }
            // — L5:  score_regiao + aprovação
            {
                std::vector<std::any> row = { sR, aprov };
                std::lock_guard<std::mutex> lk4(writeMtx4);
                outReg->addRow(row);
            }
        }
    }
};

class T9Transformer final : public Transformer {
private:
    std::mutex writeMtx;
public:
    void transform(std::vector<DataFramePtr>& outputs,
                    const std::vector<DataFrameWithIndexes>& inputs) override {
        if (inputs.size() < 2) return;
        auto inDF    = inputs[0].second;   // T3
        auto inAprov = inputs[1].second;   // T8
        auto out     = outputs[0];         // dfT9
        for (int idx : inputs[0].first) {
            StrRow row = inDF->getRow(idx);
            std::string aprovT3 = row.back();
            std::string aprovT8 = inAprov->getRow(idx).back();

            //std::cout << aprovT3 << aprovT8 << std::endl;
            if (aprovT3 != aprovT8){
                row.pop_back();
                row.push_back("0");
            }

            {
                std::lock_guard<std::mutex> lk(writeMtx);
                out->addRow(row);
            }
        }
    }
};

class T10Transformer final : public Transformer {
    private:
        std::mutex writeMtx;
    public:
        void transform(std::vector<DataFramePtr>& outputs,
                    const std::vector<DataFrameWithIndexes>& inputs) override
        {
            if (inputs.empty()) return;
            auto in  = inputs[0].second;   // dfT9
            auto out = outputs[0];         // dfT10

            // posições das colunas em dfT9
            int pUser  = in->getColumn("id_usuario_pagador")->getPosition();
            int pVal   = in->getColumn("valor_transacao")     ->getPosition();
            int pSaldo = in->getColumn("saldo")               ->getPosition();
            int pApr   = in->getColumn("aprovacao")           ->getPosition();

            // 1) acumula somatório de valor e captura o saldo por usuário
            std::unordered_map<std::string, double> sumMap;
            std::unordered_map<std::string, double> balMap;
            for (size_t r = 0; r < in->size(); ++r) {
                auto uid = in->getElement<std::string>(r, pUser);
                double v = in->getElement<double>     (r, pVal);
                double s = in->getElement<double>     (r, pSaldo);
                sumMap[uid] += v;
                balMap[uid]  = s;
            }
            //int counter = 0;
            // 2) para cada linha de entrada, decide aprovação em bloco
            for (int idx : inputs[0].first) {
                auto row = in->getRow(idx);
                auto uid = row[pUser];

                // se somatório > saldo, reprova todas as transações deste usuário
                if (sumMap[uid] > balMap[uid]) {
                    row[pApr] = "0";
                    //counter++;
                }

                std::lock_guard<std::mutex> lk(writeMtx);
                out->addRow(row);
            }
            //std::cout << "Total de transações reprovadas: " << counter << std::endl;
        }
};

class T11Transformer final : public Transformer {
    private:
        std::mutex writeMtx1;
        std::mutex writeMtx2;
    public:
        void transform(std::vector<DataFramePtr>& outputs,
                    const std::vector<DataFrameWithIndexes>& inputs) override
        {
            if (inputs.empty()) return;
            auto in        = inputs[0].second;   // dfT10
            auto outTrans  = outputs[0];         // dfT11Trans
            auto outUser   = outputs[1];         // dfT11User

            // posições das colunas em dfT10
            int pTrId    = in->getColumn("id_transacao")         ->getPosition();
            int pUser    = in->getColumn("id_usuario_pagador")   ->getPosition();
            int pVal     = in->getColumn("valor_transacao")      ->getPosition();
            int pSaldo   = in->getColumn("saldo")                ->getPosition();
            int pPixLim  = in->getColumn("limite_PIX")           ->getPosition();
            int pTedLim  = in->getColumn("limite_TED")           ->getPosition();
            int pCreLim  = in->getColumn("limite_CREDITO")       ->getPosition();
            int pBolLim  = in->getColumn("limite_Boleto")        ->getPosition();
            int pApr     = in->getColumn("aprovacao")            ->getPosition();
            int pMod     = in->getColumn("modalidade_pagamento") ->getPosition();

            // 1) Acumula novo saldo e novo limite por usuário
            std::unordered_map<std::string, double> balanceMap;
            std::unordered_map<std::string, double> limitMap;
            for (int idx : inputs[0].first) {
                auto uid = in->getElement<std::string>(idx, pUser);
                double val = in->getElement<double>(idx, pVal);
                int apr = in->getElement<int>(idx, pApr);
                std::string mod = in->getElement<std::string>(idx, pMod);

                // inicializa, na primeira ocorrência do usuário
                if (balanceMap.find(uid) == balanceMap.end()) {
                    balanceMap[uid] = in->getElement<double>(idx, pSaldo);
                    if (mod == "PIX") {
                        limitMap[uid] = in->getElement<double>(idx, pPixLim);
                    } else if (mod == "TED") {
                        limitMap[uid] = in->getElement<double>(idx, pTedLim);
                    } else if (mod == "CREDITO") {
                        limitMap[uid] = in->getElement<double>(idx, pCreLim);
                    } else if (mod == "Boleto") {
                        limitMap[uid] = in->getElement<double>(idx, pBolLim);
                    } else {
                        limitMap[uid] = 0.0;
                    }
                }

                if (apr == 1) {
                    balanceMap[uid] -= val;
                    limitMap[uid]  -= val;
                }
            }

            // 2) Emite tabela de transações: (id_transacao, aprovacao)
            for (int idx : inputs[0].first) {
                std::string trxId = in->getElement<std::string>(idx, pTrId);
                int apr = in->getElement<int>(idx, pApr);
                std::vector<std::any> row = { trxId, apr };
                {
                    std::lock_guard<std::mutex> lk1(writeMtx1);
                    outTrans->addRow(row);
                }
            }

            // 3) Emite tabela de usuários: (id_usuario_pagador, novo_saldo, novo_limite)
            for (const auto& kv : balanceMap) {
                const auto& uid = kv.first;
                double novoSaldo = kv.second;
                double novoLimite = limitMap[uid];
                std::vector<std::any> row = { uid, novoSaldo, novoLimite };
                {
                    std::lock_guard<std::mutex> lk2(writeMtx2);
                    outUser->addRow(row);
                }
            }
        }
};

class T12Transformer final : public Transformer {
public:
    void transform(std::vector<DataFramePtr>& outputs,
                    const std::vector<DataFrameWithIndexes>& inputs) override {
        std::cout << "AAAAAAAAAAAAAAAAAAAAAAa" << std::endl;
        
        if (inputs.size() < 2) return;
        auto inDF    = inputs[0].second;   // E1

        auto colID = inDF->getColumn("id_transacao");
        auto colTIM = inDF->getColumn("timestamp_envio");
        
        for (int i = 0; i < colID->size(); i++) {
            auto current_time_point = std::chrono::system_clock::now();
            long long current_timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                current_time_point.time_since_epoch()
            ).count();        
            
            std::cout << current_timestamp_ms - std::stoll(colTIM->getValue(i)) << std::endl;
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

ServerTrigger* buildPipelineTransacoes(int nThreads = 8, std::vector<VarRow>* rowBatch = nullptr) {
    //====================Construção dos DFS===========================//

    auto dfE1 = std::make_shared<DataFrame>();
    dfE1->addColumn<std::string>  ("id_transacao");
    dfE1->addColumn<std::string>  ("id_usuario_pagador");
    dfE1->addColumn<std::string>  ("id_usuario_recebedor");
    dfE1->addColumn<std::string>  ("id_regiao"); // ocorrência da região
    dfE1->addColumn<std::string>  ("modalidade_pagamento");
    dfE1->addColumn<std::string>  ("data_horario");
    dfE1->addColumn<double>       ("valor_transacao");
    dfE1->addColumn<long long int>("timestamp_envio");

    auto dfE2 = std::make_shared<DataFrame>();
    dfE2->addColumn<std::string>("id_usuario");
    dfE2->addColumn<std::string>("id_regiao"); // região do usuário mora
    dfE2->addColumn<double>     ("saldo");
    dfE2->addColumn<double>     ("limite_PIX");
    dfE2->addColumn<double>     ("limite_TED");
    dfE2->addColumn<double>     ("limite_CREDITO");
    dfE2->addColumn<double>     ("limite_Boleto");

    auto dfE3 = std::make_shared<DataFrame>();
    dfE3->addColumn<std::string>("id_regiao");
    dfE3->addColumn<double>("latitude");
    dfE3->addColumn<double>("longitude");
    dfE3->addColumn<double>("media_transacional_mensal");
    dfE3->addColumn<int>("num_fraudes_ult_30d");

    auto dfT1 = std::make_shared<DataFrame>();
    dfT1->addColumn<std::string>("id_transacao");
    dfT1->addColumn<std::string>("id_usuario_pagador");
    dfT1->addColumn<std::string>("modalidade_pagamento");
    dfT1->addColumn<double>     ("valor_transacao");
    dfT1->addColumn<double>     ("saldo");
    dfT1->addColumn<double>     ("limite_PIX");
    dfT1->addColumn<double>     ("limite_TED");
    dfT1->addColumn<double>     ("limite_CREDITO");
    dfT1->addColumn<double>     ("limite_Boleto");
    dfT1->addColumn<std::string>("data_horario");
    dfT1->addColumn<std::string>("id_regiao_transacao");
    dfT1->addColumn<std::string>("id_regiao_usuario");
    dfT1->addColumn<int>        ("aprovacao");

    auto dfT2 = dfT1->emptyCopy();
    auto dfT3 = dfT2->emptyCopy();

    auto dfT4 = std::make_shared<DataFrame>();
    dfT4->addColumn<std::string>("id_transacao");
    dfT4->addColumn<std::string>("id_regiao_transacao");
    dfT4->addColumn<double>     ("latitude_transacao");
    dfT4->addColumn<double>     ("longitude_transacao");
    dfT4->addColumn<std::string>("id_regiao_usuario");
    dfT4->addColumn<double>     ("latitude_usuario");
    dfT4->addColumn<double>     ("longitude_usuario");

    auto dfT5 = std::make_shared<DataFrame>();
    dfT5->addColumn<std::string>("id_transacao");
    dfT5->addColumn<double>     ("score_risco");

    auto dfT6 = std::make_shared<DataFrame>();
    dfT6->addColumn<std::string>("id_transacao");
    dfT6->addColumn<double>     ("score_risco");

    auto dfT7 = std::make_shared<DataFrame>();
    dfT7->addColumn<std::string>("id_transacao");
    dfT7->addColumn<double>     ("score_risco");

    auto dfT8Main = std::make_shared<DataFrame>();
    dfT8Main->addColumn<std::string>("id_transacao");
    dfT8Main->addColumn<double>     ("score_valor");
    dfT8Main->addColumn<double>     ("score_horario");
    dfT8Main->addColumn<double>     ("score_regiao");
    dfT8Main->addColumn<int>        ("aprovacao");

    auto dfT8Val = std::make_shared<DataFrame>();
    dfT8Val->addColumn<double>("score_valor");
    dfT8Val->addColumn<int>   ("aprovacao");

    auto dfT8Hor = std::make_shared<DataFrame>();
    dfT8Hor->addColumn<double>("score_horario");
    dfT8Hor->addColumn<int>   ("aprovacao");

    auto dfT8Reg = std::make_shared<DataFrame>();
    dfT8Reg->addColumn<double>("score_regiao");
    dfT8Reg->addColumn<int>   ("aprovacao");

    auto dfT9 = dfT3->emptyCopy();
    auto dfT10 = dfT9->emptyCopy();


    auto dfT11Trans = std::make_shared<DataFrame>();
    dfT11Trans->addColumn<std::string>("id_transacao");
    dfT11Trans->addColumn<int>        ("aprovacao");

    auto dfT11User  = std::make_shared<DataFrame>();
    dfT11User->addColumn<std::string>("id_usuario_pagador");
    dfT11User->addColumn<double>     ("saldo");
    dfT11User->addColumn<double>     ("limite");

    //====================Init Triggers===========================//

    auto e1 = std::make_shared<ExtractorNoop>();
    // for(VarRow row: *rowBatch){
    //     dfE1->addRow(row);
    // }
    // std::cout << dfE1->size() << std::endl;
    e1->addOutput(dfE1);
    e1->setTaskName("e1");
    e1->blockParallel();

    auto e2 = std::make_shared<ExtractorSQLite>();
    SQLiteRepository* sqliteRepository = new SQLiteRepository("data/informacoes_cadastro_100k.db");
    sqliteRepository->setTable("informacoes_cadastro");
    e2->addRepo(sqliteRepository);
    e2->addOutput(dfE2);
    e2->setTaskName("e2");
    e2->blockReadAgain();

    auto e3 = std::make_shared<ExtractorFile>();
    e3->addRepo(new FileRepository("data/regioes_estados_brasil.csv", ",", true));
    e3->addOutput(dfE3);
    e3->setTaskName("e3");
    e3->blockReadAgain();

    //==================== Construção dos elementos do DAG ===========================//
    auto t1 = std::make_shared<T1Transformer>();
    t1->addOutput(dfT1);
    t1->setTaskName("t1");

    // auto tp1 = std::make_shared<PrintTransformer>(">>> T1 outputs");
    // t1->addNext(tp1, {1});
    // tp1->setTaskName("tp1");

    auto t2 = std::make_shared<T2Transformer>();
    t2->addOutput(dfT2);
    t2->setTaskName("t2");

    // auto tp2 = std::make_shared<PrintTransformer>(">>> T2 outputs");
    // t2->addNext(tp2, {1});
    // tp2->setTaskName("tp2");

    auto t3 = std::make_shared<T3Transformer>();
    t3->addOutput(dfT3);
    t3->setTaskName("t3");

    // auto tp3 = std::make_shared<PrintTransformer>(">>> T3 outputs");
    // t3->addNext(tp3, {1});
    // // tp3->setTaskName("tp3");

    auto t4 = std::make_shared<T4Transformer>();
    t4->addOutput(dfT4);
    t4->setTaskName("t4");

    // auto tp4 = std::make_shared<PrintTransformer>(">>> T4 outputs");
    // t4->addNext(tp4, {1});
    // t4->setTaskName("tp4");

    auto t5 = std::make_shared<T5Transformer>();
    t5->addOutput(dfT5);
    t5->setTaskName("t5");

    // auto tp5 = std::make_shared<PrintTransformer>(">>> T5 outputs");
    // t5->addNext(tp5, {1});
    // tp5->setTaskName("tp5");

    auto t6 = std::make_shared<T6Transformer>();
    t6->addOutput(dfT6);
    t6->setTaskName("t6");

    // auto tp6 = std::make_shared<PrintTransformer>(">>> T6 outputs");
    // t6->addNext(tp6, {1});
    // tp6->setTaskName("tp6");

    auto t7 = std::make_shared<T7Transformer>();
    t7->addOutput(dfT7);
    t7->setTaskName("t7");

    // auto tp7 = std::make_shared<PrintTransformer>(">>> T7 outputs");
    // t7->addNext(tp7, {1});
    // tp7->setTaskName("tp7");

    auto t8 = std::make_shared<T8Transformer>();
    t8->addOutput(dfT8Main);
    t8->addOutput(dfT8Val);
    t8->addOutput(dfT8Hor);
    t8->addOutput(dfT8Reg);
    t8->setTaskName("t8");

    auto t9 = std::make_shared<T9Transformer>();
    t9->addOutput(dfT9);
    t9->setTaskName("t9");

    // auto tp9 = std::make_shared<PrintTransformer>(">>> T9 outputs");
    // t9->addNext(tp9, {1});
    // tp9->setTaskName("tp9");

    auto t10  = std::make_shared<T10Transformer>();
    t10->addOutput(dfT10);
    t10->setTaskName("t10");

    // auto tp10 = std::make_shared<PrintTransformer>(">>> T10 outputs");
    // t10->addNext(tp10, {1});
    // tp10->setTaskName("tp10");

    auto t11 = std::make_shared<T11Transformer>();
    t11->addOutput(dfT11Trans);
    t11->addOutput(dfT11User);
    t11->setTaskName("t11");

    auto t12 = std::make_shared<T12Transformer>();
    t12->setTaskName("t12");

    auto l6 = std::make_shared<LoaderFile>(0, true);
    l6->addRepo(new FileRepository("outputs/output_L6.csv", ",", true));
    l6->setTaskName("l6");

    auto l3 = std::make_shared<LoaderFile>(1, true);
    l3->addRepo(new FileRepository("outputs/output_L3.csv", ",", true));
    l3->setTaskName("l3");
    auto l4 = std::make_shared<LoaderFile>(2, true);
    l4->addRepo(new FileRepository("outputs/output_L4.csv", ",", true));
    l4->setTaskName("l4");
    auto l5 = std::make_shared<LoaderFile>(3, true);
    l5->addRepo(new FileRepository("outputs/output_L5.csv", ",", true));
    l5->setTaskName("l5");

    auto l1 = std::make_shared<LoaderFile>(0, true);
    l1->addRepo(new FileRepository("outputs/output_L1_tx.csv", ",", true));
    l1->setTaskName("l1");

    auto l2 = std::make_shared<LoaderFile>(1, true);
    l2->addRepo(new FileRepository("outputs/output_L2_usr.csv", ",", false));
    l2->setTaskName("l2");


    //==================== Construção do DAG ===========================//
    e1->addNext(t1, {1});
    e1->addNext(t12, {1});

    e2->addNext(t1, {1});

    e3->addNext(t4, {1});
    t1->addNext(t4, {1});

    t1->addNext(t2, {1});
    t2->addNext(t3, {1});

    t1->addNext(t6, {1});
    t1->addNext(t7, {1});

    t4->addNext(t5, {1});

    t3->addNext(l6, {1});
    t3->addNext(t9, {1});

    t5->addNext(t8, {1});
    t6->addNext(t8, {1});
    t7->addNext(t8, {1});

    t8->addNext(t9, {1, 1, 1, 1});
    t8->addNext(l3, {1, 1, 1, 1});
    t8->addNext(l4, {1, 1, 1, 1});
    t8->addNext(l5, {1, 1, 1, 1});

    t9->addNext(t10, {1});

    t10->addNext(t11, {1});

    t11->addNext(t12, {1,1});

    t11->addNext(l1, {1,1});
    t11->addNext(l2, {1,1});

    ServerTrigger* trigger = new ServerTrigger();
    trigger->addExtractor(e1);
    trigger->addExtractor(e2);
    trigger->addExtractor(e3);

    return trigger;
}

class TransactionServerImpl final : public TransactionService::Service {
private:
    ServerTrigger* trigger;
    PipelineManager* manager;

public:
    TransactionServerImpl(ServerTrigger* trigg, PipelineManager* man): trigger(trigg), manager(man) {};
    Status SendTransaction (ServerContext* context,
                            ServerReader<Transaction>* stream,
                            Result* reply) override {
        Transaction current;
        int incomingTransactions = 0;
        std::vector<VarRow>* rowBatch = new std::vector<VarRow>;
        while(stream->Read(&current)){
            //==== Lógica vai aqui - esse while vai loopar enquanto há stream do client ====
            VarRow row = {current.id_transacao(), current.id_usuario_pagador(),
                current.id_usuario_recebedor(), current.id_regiao(),
                current.modalidade_pagamento(), current.data_horario(),
                current.valor_transacao(), current.timestamp_envio()};
            rowBatch->push_back(row);

            // std::cout << "Thread " << std::this_thread::get_id() << " recebeu a " << incomingTransactions << "ª transação de id " << current.id_transacao() << ": " << current.id_usuario_pagador() << " | " << current.id_usuario_recebedor() << " | " << current.id_regiao() << " | " << current.modalidade_pagamento() << " | " << current.data_horario() << " | " << current.valor_transacao() << " R$" << std::endl; std::cout << rowBatch->size() << std::endl;

            incomingTransactions++;
            if(rowBatch->size() > 8000){

                std::cout << "thread " << std::this_thread::get_id() << " chamando submit." << std::endl;
                manager->submitDataBatch(*rowBatch);
                rowBatch = new std::vector<VarRow>;
            }
        }
        reply->set_ok(true);
        return Status::OK;
    }
};


void RunServer() {
    //Building dataframe
    auto dfE1 = DataFrame();
    dfE1.addColumn<std::string>  ("id_transacao");
    dfE1.addColumn<std::string>  ("id_usuario_pagador");
    dfE1.addColumn<std::string>  ("id_usuario_recebedor");
    dfE1.addColumn<std::string>  ("id_regiao"); // ocorrência da região
    dfE1.addColumn<std::string>  ("modalidade_pagamento");
    dfE1.addColumn<std::string>  ("data_horario");
    dfE1.addColumn<double>       ("valor_transacao");
    dfE1.addColumn<long long int>("timestamp_envio");

    //Building trigger and manager
    ServerTrigger* trigger = buildPipelineTransacoes();
    PipelineManager* manager = new PipelineManager(*trigger, dfE1, 8, 1);
    manager->start();

    std::string server_address("0.0.0.0:50051");
    TransactionServerImpl service(trigger, manager);
    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    server->Wait();
}

int main() {
    RunServer();
    return 0;
}

