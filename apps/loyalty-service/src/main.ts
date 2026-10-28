import { bootstrap } from "../../../libs/kafka-toolkit/bootstrap";
void bootstrap("loyalty-service", "loyalty-svc-v1").catch((error) => {
  console.error(error);
  process.exit(1);
});
