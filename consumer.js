const config = require("./config.json")[process.env.NODE_ENV || "dev"];
const { dbConnect } = require("./database/dbConnect");
var amqp = require("amqplib");
const { logger } = require("./logger");

async function consumeMobileDeletionEvent() {
  try {
    const queue = config.mobileDeletionQueue;
    const connection = await amqp.connect(config.rabbit.url);
    logger.info("RabbitMq Connected Successfully!");
    const channel = await connection.createChannel();
    channel.assertQueue(queue, { durable: true });
    channel.prefetch(1);
    let sequelize = await dbConnect();
    channel.consume(
      queue,
      async (msg) => {
        let mobile = JSON.parse(msg.content.toString());
        mobile = +mobile;
        //Search for user against mobile
        const [res1, meta] = await sequelize.query(
          `select * from kyc where mobile='${mobile}';`
        );
        let user = res1[0];

        //Delete user if found..

        if (user) {
          // -------*****************Delete the user data*************---------------------
          const [_, metadata] = await sequelize.query(
            `delete from kyc where mobile='${user.mobile}';`
          );

          if (metadata.rowCount > 0) {
            logger.info(`Successfully deleted user ... ${mobile}`);
            channel.ack(msg);
          } else {
            logger.info(`User deletion issue found for ${mobile}`);
            channel.nack(msg);
          }
          //Write producer code here.....
        } else {
          logger.info(`No user found for ${mobile}`);
          channel.ack(msg);
        }
      },
      {
        noAck: false,
      }
    );
  } catch (error) {
    logger.error(`Error Here: ${error}`);
    process.exit(1);
  }
}

consumeMobileDeletionEvent();
