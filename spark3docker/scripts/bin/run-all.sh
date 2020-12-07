echo "Starting Ingest"
spark-submit \
  --class br.com.ifood.data.featurestore.ingestion.DataIngestionStreamMain \
  /artefacts/ifood-data-ingestion.jar \
  dev \
  --kafka-topics de-order-events \
  --master-mode local[*] \
  --kafka-brokers a49784be7f36511e9a6b60a341003dc2-1378330561.us-east-1.elb.amazonaws.com:9092 \
  --output-dir /tmp/ifood/data/ \
  --stream-type order-events \
  --max-offsets-per-trigger 50000 \
  >>/tmp/ifood/ingest.log 2>&1 &

sleep 60
echo "Starting agg online"

spark-submit \
  --class br.com.ifood.data.featurestore.aggregation.AggregationsOnlineMain \
  /artefacts/ifood-data-ingestion.jar \
  dev \
  --master-mode local[*] \
  --input-data-table /tmp/ifood/data/ingestion/order-events/ \
  --output-data-table /tmp/ifood/data/aggregations/online/order-agg \
  --temp-dir tempDir \
  --window-duration "5 seconds" \
  --window-slide-duration "5 seconds" \
  --watermark "10 seconds" \
  --time-field fs_ingestion_timestamp \
  --group-field customer_id \
  >>/tmp/ifood/agg-online.log 2>&1 &

echo "Waiting streams start and generate data..."
sleep 30
echo "."
sleep 30
echo ".."
sleep 30
echo "..."
sleep 30
echo "...."
sleep 30
echo "....."
sleep 30


echo "Starting agg offline"

spark-submit \
  --class br.com.ifood.data.featurestore.aggregation.AggregationsOfflineMain \
  /artefacts/ifood-data-ingestion.jar \
  dev \
  --master-mode "local[*]" \
  --input-data-table /tmp/ifood/data/ingestion/order-events/ \
  --output-data-table /tmp/ifood/data/aggregations/offline/order-agg \
  --temp-dir tempDir \
  --time-field "order_created_at" \
  --start-date "2018-02-01" \
  --end-date "2020-12-01" \
  --group-field customer_id \
  >>/tmp/ifood/agg-offline.log 2>&1 &


echo "starting publisher historical"

spark-submit \
  --class br.com.ifood.data.featurestore.publisher.PublisherMain \
  /artefacts/ifood-data-ingestion.jar \
  dev \
  --master-mode local[*] \
  --app-name publisher \
  --publisher-type historical \
  --input-table /tmp/ifood/data/aggregations/online/order-agg \
  --output-table /tmp/ifood/data/publisher/aggregations/order-agg \
  >>/tmp/ifood/publisher.log 2>&1 &


echo "done"

