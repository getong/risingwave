-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q9_temporal_filter
AS
SELECT id,
       item_name,
       description,
       initial_bid,
       reserve,
       date_time,
       expires,
       seller,
       category,
       auction,
       bidder,
       price,
       bid_date_time
FROM (SELECT A.*,
             B.auction,
             B.bidder,
             B.price,
             B.date_time                                                                  AS bid_date_time,
             ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.date_time ASC) AS rownum
      FROM auction A,
           bid_filtered B
      WHERE A.id = B.auction
        AND B.date_time BETWEEN A.date_time AND A.expires) tmp
WHERE rownum <= 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
