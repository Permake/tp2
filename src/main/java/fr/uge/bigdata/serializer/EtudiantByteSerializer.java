package fr.uge.bigdata.serializer;

import fr.uge.bigdata.data.Etudiant;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class EtudiantByteSerializer implements Serializer<Etudiant> {
    // Il vaut mieux allouer un buffer par sérialisation ou le reset proprement
    @Override
    public byte[] serialize(String topic, Etudiant etudiant) {
        if (etudiant == null) return null;

        var firstName = etudiant.firstName().getBytes(StandardCharsets.UTF_8);
        var lastName = etudiant.lastName().getBytes(StandardCharsets.UTF_8);
        var degree = etudiant.engineeringDegree().getBytes(StandardCharsets.UTF_8);

        int size = 4 + firstName.length + 4 + lastName.length + 4 + degree.length + 4;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(firstName.length);
        buffer.put(firstName);

        buffer.putInt(lastName.length);
        buffer.put(lastName);

        buffer.putInt(degree.length);
        buffer.put(degree);

        buffer.putInt(etudiant.age());

        return buffer.array();
    }
}
