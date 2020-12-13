import org.intellij.lang.annotations.Language
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException

class MyException(message: String): Exception(message)

class DataBaseHandler(private val driver: String = "org.postgresql.Driver",
                      private val url: String = "jdbc:postgresql://localhost:5432/",
                      private val user: String = "postgres",
                      private val pass: String = "12345678",
                      private val dbName: String = "sqlcoursedatabase") {

    var connection: Connection? = null


    fun initAll() {
        try {
            connection = DriverManager.getConnection("$url$dbName?autoReconnect=true&useSSL=false", user, pass)
        } catch (se: SQLException) {
            se.printStackTrace()
            try {
                connection?.close()
            } catch (se: SQLException) {
                se.printStackTrace()
            }
        }
    }

    fun closeAll() {
        try {
            connection?.close()
        } catch (se: SQLException) {
            se.printStackTrace()
        }
    }

    fun getCourses(): List<Course>{
        @Language("PostgreSQL")
        val query = """
            select course_id, course_name from course order by course.course_name
        """.trimIndent()
        val statement = connection!!.createStatement()
        statement.execute(query)
        val rs = statement.resultSet
        val courses = mutableListOf<Course>()
        while (rs.next()) {
            val courseId = rs.getInt(1)
            val courseName = rs.getString(2)
            courses.add(courseId to courseName)
        }
        statement.close()
        return courses
    }

    fun getStudents(): List<Pair<String, String>> {
        @Language("PostgreSQL")
        val query = """
            select first_name, last_name from student;
        """.trimIndent()
        val statement = connection!!.createStatement()
        statement.execute(query)
        val rs = statement.resultSet
        val students = mutableListOf<Pair<String, String>>()
        while (rs.next()) {
            val firstName = rs.getString(1)
            val lastName = rs.getString(2)
            students.add(firstName to lastName)
        }
        statement.close()
        return students
    }

    fun getCourseGradesInfo(course: String): List<Triple<String, String, Int>>{
        @Language("PostgreSQL")
        val query = """
            select first_name, last_name, grade from student_grade join student s on
             student_grade.student_id = s.student_id
            where course_id = (select course_id from course where course_name= ?);
        """.trimIndent()
        val preparedStatement = connection!!.prepareStatement(query)
        preparedStatement.setString(1, course)
        preparedStatement.execute()
        val rs = preparedStatement.resultSet
        val studentGrades = mutableListOf<Triple<String, String, Int>>()
        while (rs.next()) {
            studentGrades.add(
                Triple(rs.getString(1), rs.getString(2), rs.getInt(3))
            )
        }
        preparedStatement.close()
        return studentGrades

    }

    fun getStudentCourseInfo(course: String, lastName: String): Int? {
        @Language("PostgreSQL")
        val query = """        
            select grade from student_grade join student s on 
            student_grade.student_id = s.student_id
            where course_id = (select course_id from course where course_name=?) and 
            last_name=?;
        """.trimIndent()
        val preparedStatement = connection!!.prepareStatement(query)
        preparedStatement.setString(1, course)
        preparedStatement.setString(2, lastName)
        preparedStatement.execute()
        val rs = preparedStatement.resultSet

        preparedStatement.close()
        return if (rs.next()) {
            rs.getInt(1)
        } else {
            null
        }
    }

    fun getStudentId(lastName: String): Int? {
        @Language("PostgreSQL")
        val query = """        
            select student_id from student where last_name=? limit 1;
        """.trimIndent()
        val preparedStatement = connection!!.prepareStatement(query)
        preparedStatement.setString(1, lastName)
        preparedStatement.execute()
        val rs = preparedStatement.resultSet
        val studentId = if (rs.next()) rs.getInt(1) else null
        preparedStatement.close()
        return studentId
    }

    fun getCourseId(course: String): Int? {
        @Language("PostgreSQL")
        val query = """        
            select course_id from course where course_name=? limit 1;
        """.trimIndent()
        val preparedStatement = connection!!.prepareStatement(query)
        preparedStatement.setString(1, course)
        preparedStatement.execute()
        val rs = preparedStatement.resultSet
        val courseId = if (rs.next()) rs.getInt(1) else null
        preparedStatement.close()
        return courseId
    }

    fun upsertStudentGrade(course: String, lastName: String, grade: Int) {
        val studentId = getStudentId(lastName) ?: throw MyException("Can't find student with last name: $lastName")
        val courseId = getCourseId(course) ?: throw MyException("Can't find course with name: $course")

        @Language("PostgreSQL")
        val query = """        
            UPDATE student_grade SET grade=? WHERE  student_id=? and course_id=?;
            INSERT INTO student_grade (student_id, course_id, grade)
            SELECT ?, ?, ?
            WHERE NOT EXISTS (SELECT 1 FROM student_grade WHERE student_id=?
                                                      and course_id=?);
        """.trimIndent()
        val preparedStatement = connection!!.prepareStatement(query)
        preparedStatement.setInt(1, grade)
        preparedStatement.setInt(2, studentId)
        preparedStatement.setInt(3, courseId)
        preparedStatement.setInt(4, studentId)
        preparedStatement.setInt(5, courseId)
        preparedStatement.setInt(6, grade)
        preparedStatement.setInt(7, studentId)
        preparedStatement.setInt(8, courseId)
        preparedStatement.execute()
        preparedStatement.close()
    }

}

typealias Course = Pair<Int, String>

fun safeCall(action: () -> Unit): Boolean {
    try {
        action()
    } catch (e: SQLException) {
        println("Ошибка SQL: ${e.printStackTrace()}")
        return false
    } catch (e: MyException) {
        println(e.message)
        return false
    }
    return true
}

fun main(args: Array<String>) {
    println("Внимание, если вы хотите использовать пробелы в названии курса, используйте вместо этого символ: _")

    print("postgres username[postgres]:")
    val username = readLine()
    print("postgres password[12345678]:")
    val password = readLine()

    val dbH = DataBaseHandler(
        user = if (username == "" || username == null) "postgres" else username,
        pass = if (password == "" || password == null) "12345678" else password
    )
    dbH.initAll()

    println("Для просмотра возможных команд введите h")

    while (true) {
        print("Введите команду:")
        val command: String = readLine() ?: ""
        val commandTokens = command.split(' ')
        if (commandTokens[0] == "h") {
            println(
                """
            Возможные команды:
            q - exit
            h - help
            c - Список курсов отсортированных по имени
            s - Фамилии и имена всех студентов.
            g course_name - Список студентов (фамилия и имя), сдавших курс и оценка по этому курсу.
            g course_name last_name - Оценку студента с фамилией last_name по курсу course_name
            u course_name last_name grade - Поставить студенту с фамилией last_name оценку grade по курсу course_name.
        """.trimIndent()
            )
        } else if (commandTokens[0] == "c") {
            safeCall {
                dbH.getCourses().forEach { (id, name) ->
                    println(name)
                }
            }
        } else if (commandTokens[0] == "s") {
            safeCall {
                dbH.getStudents().forEach { (first, last) ->
                    println("$first $last")
                }
            }
        } else if (commandTokens[0] == "g") {
            when (commandTokens.size) {
                1 -> {
                    println("Для команды g нужен аргумент course_name")
                }
                2 -> {
                    safeCall {
                        dbH.getCourseGradesInfo(commandTokens[1].replace('_', ' '))
                            .forEach { (first, last, grade) ->
                                println("$first $last: $grade")
                            }
                    }
                }
                3 -> {
                    safeCall {
                        val grade = dbH.getStudentCourseInfo(commandTokens[1].replace('_', ' '), commandTokens[2])
                        println("Оценка ${commandTokens[2]} за ${commandTokens[1]} ${grade ?: "не найдена"}")
                    }
                }
                else -> {
                    println("У команды g максимум 2 аргумента, дано ${commandTokens.size - 1}")
                }
            }
        } else if (commandTokens[0] == "u") {
            if (commandTokens.size == 4) {
                val grade = commandTokens[3].toIntOrNull()
                if (grade == null) {
                    println("Третий аргумент должен быть целым числом.")
                    continue
                }
                safeCall {
                    dbH.upsertStudentGrade(commandTokens[1].replace('_', ' '), commandTokens[2], grade)
                }
            } else {
                println("У команды u  3 аргумента, дано ${commandTokens.size}")
            }
        } else if (commandTokens[0] == "q") {
            println("Exit!")
            break
        } else {
            println("Unknown command!")
        }
    }

    dbH.closeAll()
}
