-- 创建数据库
CREATE DATABASE IF NOT EXISTS tmall_analysis;
USE tmall_analysis;


# DELETE FROM order_report;

-- 创建订单表
CREATE TABLE IF NOT EXISTS order_report (
    order_id INT PRIMARY KEY,           -- 订单编号
    total_amount DECIMAL(10,2),         -- 总金额
    paid_amount DECIMAL(10,2),          -- 买家实际支付金额
    province VARCHAR(255),              -- 收货地址
    create_time DATETIME,               -- 订单创建时间
    pay_time DATETIME,                  -- 订单付款时间
    refund_amount DECIMAL(10,2)         -- 退款金额
);

# 导入csv数据文件
LOAD DATA LOCAL INFILE 'C:/Users/10414/Desktop/GitHub 主页文件/DATA_ANALYSE/tmall-order-analysis/data/tmall_order_report.csv'
INTO TABLE order_report
-- 核心新增：指定CSV文件的编码为GBK（Windows下Excel保存的CSV默认都是这个编码）
CHARACTER SET gbk

FIELDS TERMINATED BY ','
ENCLOSED BY  '"'
LINES TERMINATED BY '\r\n'
# 跳过 CSV 文件的第一行（表头行）,替换
IGNORE 1 ROWS
(order_id, total_amount, paid_amount, province, create_time, pay_time, refund_amount);


# 数据清洗,清洗省份字段（去掉“自治区/省/维吾尔/回族/壮族”等后缀）
UPDATE order_report
SET province = REPLACE(
        REPLACE(
                REPLACE(
                        REPLACE(
                                REPLACE(province, '自治区', ''),
                                '维吾尔', ''
                        ),
                        '回族', ''
                ),
                '壮族', ''
        ),
        '省', ''
);

SELECT * FROM order_report LIMIT 15;

# 检查并删除重复数据
DELETE FROM order_report
WHERE order_id IN(
    SELECT order_id FROM (
        SELECT order_id,COUNT(*) AS cnt
        FROM order_report
        GROUP BY order_id
        HAVING cnt > 1
    ) AS t
);

# 核心指标计算
# 整体运营指标:总订单数：所有订单行数（不管有没有付款）
# 已完成订单数：付款时间不为空(0)的订单数
# 总订单金额：已付款订单的「总金额」总和
# 总实际收入金额：已付款订单的「买家实际支付金额」总和
# 退款订单数：退款金额 > 0 的订单数
# 总退款金额：已付款订单的「退款金额」总和
# 成交率 = 已完成订单数 / 总订单数
# 退货率 = 退款订单数 / 已完成订单数
SELECT
    COUNT(*) AS 总订单数量,
    SUM(CASE WHEN pay_time != 0 THEN 1 ELSE 0 END) AS 已完成订单数,
    SUM(CASE WHEN paid_amount = 0 THEN 1 ELSE 0 END) AS 未付款订单数,
    SUM(CASE WHEN refund_amount > 0 THEN 1 ELSE 0 END) AS 退款订单数,
    SUM(CASE WHEN pay_time IS NOT NULL THEN total_amount ELSE 0 END) AS 总订单金额,
    SUM(CASE WHEN pay_time IS NOT NULL THEN refund_amount ELSE 0 END) AS 总退款金额,
    SUM(CASE WHEN pay_time IS NOT NULL THEN paid_amount ELSE 0 END) AS 总实际收入金额,
    CONCAT(ROUND(SUM(CASE WHEN pay_time != 0 THEN 1 ELSE 0 END)/COUNT(*)*100,2),'%') AS 成交率,
    CONCAT(ROUND(SUM(CASE WHEN refund_amount > 0 THEN 1 ELSE 0 END)/SUM(CASE WHEN pay_time != 0 THEN 1 ELSE 0 END)*100,2),'%') AS 退货率
FROM order_report;

# 地区订单分布
SELECT
    province AS 省份,
    COUNT(*) AS 订单量
FROM order_report
WHERE pay_time != 0
GROUP BY province
ORDER BY 订单量 DESC;

# 时间趋势分析
-- 每日订单量
SELECT
    DATE_FORMAT(create_time, '%Y-%m-%d') AS 日期,
    COUNT(*) AS 订单量
FROM order_report
GROUP BY 日期
ORDER BY 日期;

-- 每小时订单量
SELECT
    DATE_FORMAT(create_time, '%H') AS 小时,
    COUNT(*) AS 订单量
FROM order_report
GROUP BY 小时
ORDER BY 小时;

-- 下单到付款平均耗时（分钟）
SELECT
    AVG(TIMESTAMPDIFF(MINUTE, create_time, pay_time)) AS 平均付款耗时_分钟
FROM order_report
WHERE pay_time IS NOT NULL;
