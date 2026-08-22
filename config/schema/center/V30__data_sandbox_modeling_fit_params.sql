-- Sandbox modeling: persist per-node preprocessing fit params (智能建模拟合参数持久化).
--
-- ds_compute_node_run.fit_params: 预处理算子（fillna/outlier/standardize/binning/unique/derive）
-- 执行时回传的拟合参数（JSON），如 standardize 的 mean/std、outlier 的 IQR 截断边界。
-- 训练节点注册模型制品时，依据上游链的拟合参数生成自包含 predict 脚本（复刻 原始输入→模型特征 的变换），
-- 保证 API 推理与画布训练特征一致（修复标准化特征训练、原始特征推理导致预测退化的缺陷）。

alter table ds_compute_node_run add column fit_params text default '';
