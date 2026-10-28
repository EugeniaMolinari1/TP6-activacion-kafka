import { bootstrap } from "../../../libs/kafka-toolkit/bootstrap";
void bootstrap("notification-service", "notification-svc").catch((error) => {
  console.error(error);
  process.exit(1);
});
