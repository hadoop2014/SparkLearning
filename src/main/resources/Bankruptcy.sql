CREATE EXTERNAL TABLE IF NOT EXISTS Bankruptcy(
  IndustrialRisk STRING,
  ManagementRisk STRING,
  FinancialFlexibility STRING,
  Credibility STRING,
  Competitiveness STRING,
  OperatingRisk STRING,
  Class STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 'hdfs://master:9000/data/Bankruptcy';

--(P=Positive,A-Average,N-negative,B-Bankruptcy,NB-Non-Bankruptcy)
--     1. Industrial Risk: {P,A,N}                工业风险    
--     2. Management Risk: {P,A,N}                管理风险
--     3. Financial Flexibility: {P,A,N}          财务灵活性
--     4. Credibility: {P,A,N}                    信誉
--     5. Competitiveness: {P,A,N}                竞争力
--     6. Operating Risk: {P,A,N}                 经营风险
--     7. Class: {B,NB}                           分类
 






