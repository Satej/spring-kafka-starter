package io.confluent.developer.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringCcloudApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCcloudApplication.class, args);
	}

}

 @RequiredArgsConstructor
 @Component
 class Producer {

 	private final KafkaTemplate<Integer, String> template;

   	Faker faker;

   	@EventListener(ApplicationStartedEvent.class)
   	public void generate() {

    		faker = Faker.instance();
     		final Flux<Long> interval = Flux.interval(Duration.ofMillis(1_000));

    	final Flux<String> quotes = Flux.fromStream(Stream.generate(() -> faker.hobbit().quote()));

     	Flux.zip(interval, quotes)
      		.map(it -> template.send("hobbit", faker.random().nextInt(42), it.getT2())).blockLast();
   	}
 }