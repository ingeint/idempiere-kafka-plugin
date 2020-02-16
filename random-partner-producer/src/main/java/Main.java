import com.github.javafaker.Faker;
import presenter.Partner;
import producer.PartnerKafkaProducer;

import java.util.UUID;

public class Main {

    public static void main(String[] args) {
        Faker faker = new Faker();

        Partner partner = new Partner();
        partner.setUu(UUID.randomUUID().toString());
        partner.setName(faker.name().fullName());
        partner.setValue(faker.idNumber().valid());

        PartnerKafkaProducer producer = new PartnerKafkaProducer();
        producer.send(partner);
    }
}
