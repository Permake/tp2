package fr.uge.bigdata.serializer;

import fr.uge.bigdata.data.Etudiant;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.databind.ObjectMapper;

public class EtudiantDeserializer implements Deserializer<Etudiant> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public Etudiant deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, Etudiant.class);
        } catch (Exception e) {
            return null;
        }
    }
}
