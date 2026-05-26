package io.javabytes.batch.recon.grade;

public record Student(String name, int score, String grade) {
    // Helper to create a new student with a calculated grade
    
    public Student(String name, int score){
        this(name, score, null);
    }
    
    public Student withGrade(String calculatedGrade) {
        return new Student(this.name, this.score, calculatedGrade);
    }
}
