package fr.uge.bigdata.serializer;

import fr.uge.bigdata.data.Etudiant;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.databind.ObjectMapper;

public class EtudiantSerializer implements Serializer<Etudiant> {
    private final ObjectMapper mapper = new ObjectMapper();
    @Override
    public byte[] serialize(String topic, Etudiant etudiant) {
        try {
            return mapper.writeValueAsBytes(etudiant);
        } catch (Exception e) {
            throw new RuntimeException("Error when serializing etudiant " + etudiant);
        }
    }
}
