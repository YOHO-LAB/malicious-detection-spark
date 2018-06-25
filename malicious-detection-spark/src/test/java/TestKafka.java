import com.yoho.bigdata.spark.model.parser.MessageParser;

/**
 * Created by markeloff on 17/2/16.
 */
public class TestKafka {
    public static void main(String[] args) {
        String msg = "{\"@timestamp\":\"2017-08-23T11:50:17.740Z\",\"beat\":{\"hostname\":\"VM_0_240_centos\",\"name\":\"VM_0_240_centos\",\"version\":\"5.5.2\"},\"input_type\":\"log\",\"message\":\"10.66.0.240|113.241.65.206|2017-08-23 19:50:17|GET|YOHO!Buy/6.0.0.384 ( Model/OPPO R9s;OS/6.0.1;Channel/3117;Resolution/1920*1080;Udid/8644160323015939a8d2d227010774d )|app.consult.lastTwo|fromPage=aFP_ProductDetail\\u0026v=7\\u0026uid=51435694\\u0026udid=8644160323015939a8d2d227010774d\\u0026physical_channel=2\\u0026client_type=android\\u0026session_key=abd2259305c88aa60c0eb1e52e0dfdcd\\u0026client_secret=f3425c4a66858fdeaaada9ed97a135f8\\u0026method=app.consult.lastTwo\\u0026limit=3\\u0026gender=2%2C3\\u0026productId=803480\\u0026os_version=android6.0.1%3AOPPO_R9s\\u0026app_version=6.0.0\\u0026screen_size=1080x1920\\u0026yh_channel=2|200|1\",\"offset\":8145992,\"source\":\"/Data/logs/gateway/gateway_access.log\",\"tags\":[\"gateway_access\",\"gateway_access\"],\"type\":\"gateway_access\"}";
        System.out.println(MessageParser.parse( msg));

    }


}


