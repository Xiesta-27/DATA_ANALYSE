# 双十一淘宝美妆数据.csv 这个数据集是美妆店铺的双十一销售数据，可以挖掘的纬度有日期、店铺，
# 指标则有销售量、销售额、评论数等。

# 创建存储双十一美妆数据的数据库与表
CREATE DATABASE IF NOT EXISTS double11_beauty_analysis;
# 并切换到该数据库
USE double11_beauty_analysis;


# 创建CSV数据表,根据CSV文件的列名和数据类型，创建空表用于存储数据
# 先将update_time存为字符串，后续再转日期格式；price用DECIMAL保证精度
CREATE TABLE IF NOT EXISTS  beauty_sales (
    update_time VARCHAR(20) COMMENT  '数据更新日期',  -- 将update_time转换为原始字符串格式
    id  VARCHAR(50) NOT NULL COMMENT '商品ID',
    title TEXT COMMENT '商品标题',
    price DECIMAL(10, 2) COMMENT '商品单价',
    sale_count BIGINT COMMENT '销售量',
    comment_count BIGINT COMMENT '评论数',
    shop_name VARCHAR(100) COMMENT '店铺名称'
) COMMENT '2016年双十一淘宝美妆销售数据';

# TRUNCATE TABLE beauty_sales;

# SQL 代码自动化导入 CSV 数据到beauty_sales表
SET GLOBAL local_infile = 1; -- 开启 MySQL 本地文件导入权限

LOAD DATA LOCAL INFILE 'C:/Users/10414/Desktop/GitHub 主页文件/DATA_ANALYSE/数据集/OrderFromTmall/双十一淘宝美妆数据.csv'
INTO TABLE beauty_sales
FIELDS  TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(update_time, id, title, price, sale_count, comment_count, @shop_name)
# 这里将最后一列 '店名' 先放进临时变量 @shop_name
SET shop_name = @shop_name;  -- 再将临时变量赋值给表字段

# 查看前5行数据,验证数据是否成功导入
SELECT * FROM beauty_sales LIMIT 5;

# 查看数据集总行数
SELECT COUNT(1) AS total_raws FROM beauty_sales;
# 查看表结构,字段类型\非空约束
DESCRIBE beauty_sales;


# 重复数据检查与删除:先统计“所有字段完全重复”的行数
SELECT COUNT(*) AS duplicate_count
FROM (
    SELECT update_time, id, title, price, sale_count, comment_count, shop_name
    FROM beauty_sales
    GROUP BY update_time, id, title, price, sale_count, comment_count, shop_name
    HAVING COUNT(1) > 1
) t ;

# 删除重复数据，保留唯一行（用窗口函数ROW_NUMBER）
# 创建临时表，再替换原表
CREATE TABLE beauty_sales_new  AS
SELECT update_time, id, title, price, sale_count, comment_count, shop_name
FROM (
    SELECT *,
        ROW_NUMBER()OVER(
            PARTITION BY update_time, id, title, price, sale_count, comment_count, shop_name
            ORDER BY (SELECT NULL) -- 不需要对分区内的数据做任何实际排序，随便给我一个顺序就行，只要能生成连续的行号即可
            ) AS rn
    FROM beauty_sales
) t
WHERE rn = 1 ;-- 只保留每组的第1行

-- 先删除原表
DROP TABLE beauty_sales;

-- 再将临时表重命名为原表名
RENAME TABLE beauty_sales_new TO beauty_sales;

-- 验证去重结果
SELECT COUNT(*) AS total_count FROM beauty_sales;
# 原本数据量为27598，去重后变为27512

# 进行空值检查与填充
SELECT
    COUNT(*) - COUNT(update_time) AS update_time_null,
    COUNT(*) - COUNT(id) AS id_null,
    COUNT(*) - COUNT(title) AS title_null,
    COUNT(*) - COUNT(price) AS price_null,
    COUNT(*) - COUNT(sale_count) AS sale_count_null,
    COUNT(*) - COUNT(comment_count) AS comment_count_null,
    COUNT(*) - COUNT(shop_name) AS shop_name_null
FROM beauty_sales;
# 检测发现无空值，省略步骤:将空值填充为0

#  日期格式化：将update_time从“YYYY/MM/DD”字符串转为“YYYY-MM-DD”日期格式
CREATE TABLE beauty_sales_date  AS
SELECT
    DATE_FORMAT(STR_TO_DATE(beauty_sales.update_time,'%Y/%m/%d'),'%Y-%m-%d') AS update_time,
    id,
    title,
    price,
    sale_count,
    comment_count,
    shop_name
FROM beauty_sales;

-- 先删除原表
DROP TABLE beauty_sales;
-- 再将临时表重命名为原表名
RENAME TABLE beauty_sales_date TO beauty_sales;

-- 验证日期格式结果
SELECT update_time FROM beauty_sales LIMIT 10 ;
# 可以看出原来的 2016/11/10 格式已经被成功转换为 2016-11-10 的标准日期格式

# 计算销售额新字段:通过“单价 × 销量”计算销售额，新增sale_amount字段
CREATE  TABLE beauty_sales_sale_amount AS
SELECT  *,
    price * sale_count AS sale_amount  -- 直接计算并新增字段
FROM beauty_sales;

-- 先删除原表
DROP TABLE beauty_sales;
-- 再将临时表重命名为原表名
RENAME TABLE beauty_sales_sale_amount TO beauty_sales;
-- 验证sale_amount结果
SELECT sale_amount FROM beauty_sales LIMIT 10 ;


# 结果验证与简单分析
-- 查看销量最低的5个商品（验证数据清洗结果）
SELECT *
FROM beauty_sales
WHERE sale_count > 0
ORDER BY sale_count ASC
LIMIT 5;

-- 查看销量最高的5个商品（观察爆款）
SELECT *
FROM beauty_sales
WHERE sale_count > 0
ORDER BY sale_count DESC
LIMIT 5;

-- 查看最终表结构（确认所有字段都已生成）
DESCRIBE beauty_sales;