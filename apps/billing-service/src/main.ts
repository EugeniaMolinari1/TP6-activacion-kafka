import { bootstrap } from "../../../libs/kafka-toolkit/bootstrap";
void bootstrap("billing-service", "billing-svc").catch((error) => {
  console.error(error);
  process.exit(1);
});
