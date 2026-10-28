import { bootstrap } from "../../../libs/kafka-toolkit/bootstrap";
void bootstrap("crm-analytics-service", "crm-analytics").catch((error) => {
  console.error(error);
  process.exit(1);
});
