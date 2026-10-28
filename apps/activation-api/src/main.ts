import { bootstrap } from "../../../libs/kafka-toolkit/bootstrap";
void bootstrap("activation-api", "activation-api").catch((error) => {
  console.error(error);
  process.exit(1);
});
