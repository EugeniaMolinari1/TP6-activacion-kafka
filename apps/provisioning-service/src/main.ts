import { bootstrap } from "../../../libs/kafka-toolkit/bootstrap";
void bootstrap("provisioning-service", "provisioning-svc").catch((error) => {
  console.error(error);
  process.exit(1);
});
