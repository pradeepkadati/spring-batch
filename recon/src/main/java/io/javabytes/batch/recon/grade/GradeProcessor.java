package io.javabytes.batch.recon.grade;

import org.springframework.batch.item.ItemProcessor;

public class GradeProcessor implements ItemProcessor<Student, Student> {
    @Override
    public Student process(Student student) {
        String letterGrade;
        if (student.score() >= 90) letterGrade = "A";
        else if (student.score() >= 80) letterGrade = "B";
        else if (student.score() >= 70) letterGrade = "C";
        else letterGrade = "F";

        System.out.println("Processing " + student.name() + ": " + letterGrade);
        return student.withGrade(letterGrade);
    }

}
