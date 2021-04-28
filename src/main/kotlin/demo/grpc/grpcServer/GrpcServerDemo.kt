package demo.grpc.grpcServer

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.*
import com.amazonaws.services.dynamodbv2.model.*
import com.google.rpc.Code
import demo.grpc.DemoGrpcTestsRequest
import demo.grpc.DemoGrpcTestsResponse
import demo.grpc.DemoGrpcTestsServiceGrpc
import demo.grpc.ErrorDetails
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.math.BigInteger
import javax.inject.Singleton

@Singleton
class GrpcServerDemo : DemoGrpcTestsServiceGrpc.DemoGrpcTestsServiceImplBase() {

    val logger = LoggerFactory.getLogger(this::class.java)

    val PRIMARY_KEY: String = "pk_contato"
    val SORT_KEY: String = "sk#contato"
    val TABLE_NAME: String = "contatos"

    override fun saveDynamoDb(
        request: DemoGrpcTestsRequest?,
        responseObserver: StreamObserver<DemoGrpcTestsResponse>?
    ) {
        responseObserver ?: throw RuntimeException()
        request ?: throw RuntimeException()

        logger.info("Criando client builder do dynamoDB local.")
        val clientBuilder = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "local"))
            .build()

        logger.info("Criando client do dynamoDB.")
        val clientDB = DynamoDB(clientBuilder)

        try {
            logger.info("Criando estrutura da tabela '$TABLE_NAME' dynamoDB.")
            val request = CreateTableRequest()
                .withTableName(TABLE_NAME)
                .withAttributeDefinitions(AttributeDefinition(PRIMARY_KEY, ScalarAttributeType.S))
                .withAttributeDefinitions(AttributeDefinition("sk#contato", ScalarAttributeType.S))
                .withKeySchema(
                    listOf(
                        KeySchemaElement(PRIMARY_KEY, KeyType.HASH),
                        KeySchemaElement(SORT_KEY, KeyType.RANGE)
                    )
                )
                //https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.ProvisionedThroughput.Manual
                .withBillingMode(BillingMode.PAY_PER_REQUEST)

            logger.info("Criando efetivamente tabela.")
            clientDB.createTable(request).waitForActive()

            logger.info("Listagem de tabelas no banco.")
            clientDB.listTables().map {
                logger.info(it.tableName.toString())
            }

            logger.info("Criando itens para cadastro no dynamoDB.")
            val item: Item = Item()
                .withPrimaryKey(PRIMARY_KEY, "1") //Campo obrigátorio
                .withPrimaryKey(SORT_KEY, "sk#1") //Campo obrigátorio
                .withJSON(
                    "info",
                    """
                    {"testeParam": "Valor",
                    "teste2": "Adicionado um valor"}
                """.trimIndent()
                )
            val item1: Item = Item()
                .withPrimaryKey(PRIMARY_KEY, "1") //Campo obrigátorio
                .withPrimaryKey(SORT_KEY, "sk#2") //Campo obrigátorio
                .withJSON(
                    "info",
                    """
                    {"testeParam": "Valor",
                    "teste2": "Adicionado um valor"}
                """.trimIndent()
                )
            logger.info("Item 1: ${item}")

            val item2: Item = Item()
                .withPrimaryKey(PRIMARY_KEY, "2") //Campo obrigátorio
                .withPrimaryKey(SORT_KEY, "sk#3") //Campo obrigátorio
                .withJSON(
                    "info",
                    """
                    {"testeParam": "Valor",
                    "teste2": "Adicionado mais um campo"}
                """.trimIndent()
                )
                .withString("nome", "Matheus")
                .withBigInteger("idade", BigInteger.valueOf(19))
                .withJSON(
                    "credenciais",
                    """
                    {
                        "email": "email@gmail.com",
                        "senha": "senhaSegura"
                    }
                """.trimIndent()
                )
            logger.info("Item 2: ${item2}")

            logger.info("Pegando a tabela para realizar o cadastro.")
            val table: Table = clientDB.getTable(TABLE_NAME)

            logger.info("Inserindo os itens.")
            table.putItem(item)
            table.putItem(item1)
            table.putItem(item2)
            logger.info("Itens inseridos.")

            logger.info("-----Realizando consulta no banco-----")
            logger.info("-----getItem-----")
            val busca = clientDB.getTable(TABLE_NAME).getItem(PRIMARY_KEY, "1", SORT_KEY, "sk#1")
            logger.info("Dado consultado: ${busca}")

            logger.info("-----query.-----")
            val queryByPkOnly = clientDB.getTable(TABLE_NAME).query(
                KeyAttribute(PRIMARY_KEY, "1")
            )
            val queryByPkAndSkBeginsWith = clientDB.getTable(TABLE_NAME).query(
                KeyAttribute(PRIMARY_KEY, "1"),
                RangeKeyCondition(SORT_KEY).beginsWith("sk#")
            )
            logger.info("Buscando por pk e sk: ${queryByPkAndSkBeginsWith.map{ it.asMap() }}")

            val response = DemoGrpcTestsResponse.newBuilder()
                .setMessage("Elemento criado.")
                .build()
            responseObserver.onNext(response)
            responseObserver.onCompleted()
        }catch (exp: Exception){
            logger.error("Ocorreu um erro. Será tratado genéricamente.")
            val statusProto = com.google.rpc.Status.newBuilder()
                .setCode(Code.INTERNAL.number)
                .setMessage(exp.message)
                .addDetails(
                    com.google.protobuf.Any.pack(
                        ErrorDetails.newBuilder()
                        .setCode(401)
                        .setMessage(exp.message)
                        .build())
                )
                .build()
            responseObserver.onError(io.grpc.protobuf.StatusProto.toStatusRuntimeException(statusProto))
        }finally {
            logger.info("Deletando a tabela e retornando a mensagem. (Comente essa linha para verificar os dados no NoSqlWorkbench).")
            clientDB.getTable(TABLE_NAME).delete()
        }
    }

}