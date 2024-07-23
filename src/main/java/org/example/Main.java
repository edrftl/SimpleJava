package org.example;

import com.github.javafaker.Faker;

import java.sql.*;
import java.util.Scanner;

public class Main {

    private static final String URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String USER = "postgres";
    private static final String PASSWORD = "123456";

    private static Connection connection;

    public static void main(String[] args) {
        try {
            connection = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("Database connected successfully!");

            var command = connection.createStatement();

            while (true) {
                Scanner scanner = new Scanner(System.in);

                System.out.print("1 - createTableAnimals\n" +
                        "2 - Insert Animal\n" +
                        "3 - Delete Animal\n" +
                        "4 - Update Animal\n" +
                        "5 - Show Animal\n" +
                        "6 - Generate and Insert 100,000 Animals\n" +
                        "0 - Close\n" +
                        "Enter command ->_");
                int menu = scanner.nextInt();
                switch (menu) {
                    case 0:
                        return;
                    case 1:
                        createTableAnimals(command);
                        break;
                    case 2:
                        insertAnimal();
                        break;
                    case 3:
                        deleteAnimal(connection);
                        break;
                    case 4:
                        updateAnimal(connection);
                        break;
                    case 5:
                        showAnimal(command);
                        break;
                    case 6:
                        generateAndInsertAnimals();
                        break;
                }
            }
        } catch (SQLException e) {
            System.out.println("Begin working" + e.getMessage());
        }
    }

    private static void createTableAnimals(Statement command) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS animals (" +
                "id SERIAL PRIMARY KEY, " +
                "name VARCHAR(50) NOT NULL, " +
                "species VARCHAR(50) NOT NULL, " +
                "age INT NOT NULL, " +
                "weight DECIMAL(5, 2) NOT NULL" +
                ")";
        command.executeUpdate(sql);
    }

    private static void showAnimal(Statement command) throws SQLException {
        String sql = "SELECT * FROM animals";

        var resultSet = command.executeQuery(sql);

        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            String species = resultSet.getString("species");
            int age = resultSet.getInt("age");
            double weight = resultSet.getDouble("weight");

            System.out.println("ID: " + id + ", Name: " + name + ", Species: " + species + ", Age: " + age + ", Weight: " + weight);
        }
        resultSet.close();
    }

    private static void insertAnimal() throws SQLException {
        Animal animal = new Animal();

        Scanner scanner = new Scanner(System.in);
        System.out.print("Вкажіть назву ->_");
        animal.setName(scanner.nextLine());

        System.out.print("Вкажіть вид тварини ->_");
        animal.setSpecies(scanner.nextLine());

        System.out.print("Вкажіть вік ->_");
        animal.setAge(scanner.nextInt());

        System.out.print("Вкажіть вагу ->_");
        animal.setWeight(scanner.nextDouble());

        String sql = "INSERT INTO animals (name, species, age, weight) VALUES (?, ?, ?, ?)";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, animal.getName());
        preparedStatement.setString(2, animal.getSpecies());
        preparedStatement.setInt(3, animal.getAge());
        preparedStatement.setBigDecimal(4, java.math.BigDecimal.valueOf(animal.getWeight()));

        int rowsInserted = preparedStatement.executeUpdate();
        if (rowsInserted > 0) {
            System.out.println("Тваринку успішно додано!");
        }
        preparedStatement.close();
    }

    private static void deleteAnimal(Connection conn) throws SQLException {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Вкажіть id ->_");
        long id = scanner.nextLong();

        String sql = "DELETE FROM animals WHERE id=?";

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setLong(1, id);

        int rowsDeleted = statement.executeUpdate();
        if (rowsDeleted > 0) {
            System.out.println("A animal was deleted successfully!");
        } else {
            System.out.println("No animal found with the specified id.");
        }

        statement.close();
    }

    private static void updateAnimal(Connection conn) throws SQLException {
        Animal animal = new Animal();
        Scanner scanner = new Scanner(System.in);

        System.out.print("Вкажіть id ->_");
        int id = scanner.nextInt();
        scanner.nextLine();  // Consume newline

        System.out.print("Вкажіть назву ->_");
        animal.setName(scanner.nextLine());

        System.out.print("Вкажіть вид тварини ->_");
        animal.setSpecies(scanner.nextLine());

        System.out.print("Вкажіть вік ->_");
        animal.setAge(scanner.nextInt());

        System.out.print("Вкажіть вагу ->_");
        animal.setWeight(scanner.nextDouble());

        String sql = "UPDATE animals SET name=?, species=?, age=?, weight=? WHERE id=?";

        PreparedStatement statement = conn.prepareStatement(sql);
        statement.setString(1, animal.getName());
        statement.setString(2, animal.getSpecies());
        statement.setInt(3, animal.getAge());
        statement.setDouble(4, animal.getWeight());
        statement.setInt(5, id);

        int rowsUpdated = statement.executeUpdate();
        if (rowsUpdated > 0) {
            System.out.println("An existing user was updated successfully!");
        } else {
            System.out.println("No user found with the specified id.");
        }
        statement.close();
    }

    private static void generateAndInsertAnimals() throws SQLException {
        Faker faker = new Faker();

        String sql = "INSERT INTO animals (name, species, age, weight) VALUES (?, ?, ?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        for (int i = 0; i < 100000; i++) {
            String name = faker.name().firstName();
            String species = faker.animal().name();
            int age = faker.number().numberBetween(1, 20);
            double weight = faker.number().randomDouble(2, 1, 100);

            preparedStatement.setString(1, name);
            preparedStatement.setString(2, species);
            preparedStatement.setInt(3, age);
            preparedStatement.setDouble(4, weight);

            preparedStatement.addBatch();
            if (i % 1000 == 0) { // Execute every 1000 inserts to improve performance
                preparedStatement.executeBatch();
            }
        }

        // Execute any remaining inserts
        preparedStatement.executeBatch();

        System.out.println("100,000 animal records have been generated and inserted.");
        preparedStatement.close();
    }
}
