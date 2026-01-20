package fr.uge.bigdata.serializer;

import fr.uge.bigdata.data.Etudiant;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class EtudiantByteDeserializer implements Deserializer<Etudiant> {
    @Override
    public Etudiant deserialize(String topic, byte[] data) {
        if (data == null) return null;

        var buffer = ByteBuffer.wrap(data);

        // Lecture Prénom
        var firstNameSize = buffer.getInt();
        byte[] firstNameBytes = new byte[firstNameSize];
        buffer.get(firstNameBytes);
        var firstName = new String(firstNameBytes, StandardCharsets.UTF_8);

        // Lecture Nom
        var lastNameSize = buffer.getInt();
        byte[] lastNameBytes = new byte[lastNameSize];
        buffer.get(lastNameBytes);
        var lastName = new String(lastNameBytes, StandardCharsets.UTF_8);

        // Lecture Diplôme
        var engineeringDegreeSize = buffer.getInt();
        byte[] degreeBytes = new byte[engineeringDegreeSize];
        buffer.get(degreeBytes);
        var engineeringDegree = new String(degreeBytes, StandardCharsets.UTF_8);

        // Lecture Age
        var age = buffer.getInt();

        return new Etudiant(firstName, lastName, age, engineeringDegree);
    }
}