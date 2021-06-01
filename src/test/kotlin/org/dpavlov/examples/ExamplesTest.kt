package org.dpavlov.examples

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.ints.shouldBeGreaterThanOrEqual
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeSameInstanceAs
import org.jetbrains.exposed.dao.*
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IntIdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.DriverManager

internal class Examples : FunSpec({

    test("successful connection") {
        val database = Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            database.version shouldBe 1.4.toBigDecimal()
            database.vendor shouldBe "h2"
        }
    }

    test("successful connection with credentials") {
        val database =
            Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver", user = "myself", password = "secret")
        transaction {
            database.version shouldBe 1.4.toBigDecimal()
            database.vendor shouldBe "h2"
        }
    }

    test("successful manual connection ") {
        var connected = false
        Database.connect({ connected = true; DriverManager.getConnection("jdbc:h2:mem:test;MODE=MySQL") })
        connected shouldBe false
        transaction {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(Cities)
            connected shouldBe true
        }
    }

    test("when insert then generated key") {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(StarWarsFilms)
            val id = StarWarsFilms.insertAndGetId {
                it[name] = "The Last Jedi"
                it[sequelId] = 8
                it[director] = "Rian Johnson"
            }
            id.value shouldBe 1

            val insert = StarWarsFilms.insert {
                it[name] = "The Force Awakens"
                it[sequelId] = 7
                it[director] = "J.J. Abrams"
            }
            insert[StarWarsFilms.id].value shouldBe 2

            val selectAll = StarWarsFilms.selectAll()
            selectAll.forEach {
                it[StarWarsFilms.sequelId] shouldBeGreaterThanOrEqual 7
            }

            StarWarsFilms.slice(StarWarsFilms.name, StarWarsFilms.director).selectAll()
                .forEach {
                    it[StarWarsFilms.name].startsWith("The") shouldBe true
                }

            val select =
                StarWarsFilms.select { (StarWarsFilms.director like "J.J.%") and (StarWarsFilms.sequelId eq 7) }
            select.count() shouldBe 1

            StarWarsFilms.update({ StarWarsFilms.sequelId eq 8 }) {
                it[name] = "Episode VIII – The Last Jedi"
                with(SqlExpressionBuilder) {
                    it.update(sequelId, sequelId + 1)
                }
            }
        }
    }

    test("count distinct example") {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(StarWarsFilms)

            val batchInsertObjects = listOf(
                InsertItem("The Last Jedi", 8, "Rian Johnson"),
                InsertItem("The Force Awakens", 7, "J.J. Abrams"),
                InsertItem("Episode I – The Phantom Menace", 9, "George Lucas"),
            )

            StarWarsFilms.batchInsert(batchInsertObjects) { item ->
                this[StarWarsFilms.name] = item.name
                this[StarWarsFilms.sequelId] = item.sequelId
                this[StarWarsFilms.director] = item.director
            }

            StarWarsFilms.slice(StarWarsFilms.name.countDistinct()).selectAll().forEach {
                println(it[StarWarsFilms.name.countDistinct()])
            }
        }
    }

    test("distinct filtering") {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(StarWarsFilms)

            val batchInsertObjects = listOf(
                InsertItem("The Last Jedi", 8, "Rian Johnson"),
                InsertItem("The Force Awakens", 7, "George Lucas"),
                InsertItem("Episode I – The Phantom Menace", 9, "George Lucas"),
            )

            StarWarsFilms.batchInsert(batchInsertObjects) { item ->
                this[StarWarsFilms.name] = item.name
                this[StarWarsFilms.sequelId] = item.sequelId
                this[StarWarsFilms.director] = item.director
            }

            val query = StarWarsFilms.slice(StarWarsFilms.director).selectAll().withDistinct(true)
            query.count() shouldBe 2
        }
    }

    test("order by") {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(StarWarsFilms)

            val batchInsertObjects = listOf(
                InsertItem("The Last Jedi", 8, "Rian Johnson"),
                InsertItem("The Force Awakens", 7, "J.J. Abrams"),
                InsertItem("Episode I – The Phantom Menace", 9, "George Lucas"),
            )

            StarWarsFilms.batchInsert(batchInsertObjects) { item ->
                this[StarWarsFilms.name] = item.name
                this[StarWarsFilms.sequelId] = item.sequelId
                this[StarWarsFilms.director] = item.director
            }

            StarWarsFilms.selectAll().orderBy(StarWarsFilms.sequelId).forEach {
                println(it[StarWarsFilms.sequelId])
            }
        }
    }

    test("group by director") {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(StarWarsFilms)

            val batchInsertObjects = listOf(
                InsertItem("The Last Jedi", 8, "Rian Johnson"),
                InsertItem("The Force Awakens", 7, "George Lucas"),
                InsertItem("Episode I – The Phantom Menace", 9, "George Lucas"),
            )

            StarWarsFilms.batchInsert(batchInsertObjects) { item ->
                this[StarWarsFilms.name] = item.name
                this[StarWarsFilms.sequelId] = item.sequelId
                this[StarWarsFilms.director] = item.director
            }

            val query = StarWarsFilms.slice(StarWarsFilms.sequelId.count(), StarWarsFilms.director)
                .selectAll()
                .groupBy(StarWarsFilms.director)

            query.forEach {
                println(it)
            }
        }
    }

    test("when foreign ket then auto join") {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(StarWarsFilms, Players)
            StarWarsFilms.insert {
                it[name] = "The Last Jedi"
                it[sequelId] = 8
                it[director] = "Rian Johnson"
            }
            StarWarsFilms.insert {
                it[name] = "The Force Awakens"
                it[sequelId] = 7
                it[director] = "J.J. Abrams"
            }
            Players.insert {
                it[name] = "Mark Hamill"
                it[sequelId] = 7
            }
            Players.insert {
                it[name] = "Mark Hamill"
                it[sequelId] = 8
            }

            val simpleInnerJoin = (StarWarsFilms innerJoin Players).selectAll()
            simpleInnerJoin.count() shouldBe 2
            simpleInnerJoin.forEach {
                it[StarWarsFilms.name] shouldNotBe null
                it[StarWarsFilms.sequelId] shouldBe it[Players.sequelId]
                it[Players.name] shouldBe "Mark Hamill"
            }

            val innerJoinWithCondition = (StarWarsFilms innerJoin Players)
                .select { StarWarsFilms.sequelId eq Players.sequelId }
            innerJoinWithCondition.count() shouldBe 2

            innerJoinWithCondition.forEach {
                it[StarWarsFilms.name] shouldNotBe null
                it[StarWarsFilms.sequelId] shouldBe it[Players.sequelId]
                it[Players.name] shouldBe "Mark Hamill"
            }

            val complexInnerJoin = Join(
                table = StarWarsFilms,
                otherTable = Players,
                joinType = JoinType.INNER,
                onColumn = StarWarsFilms.sequelId,
                otherColumn = Players.sequelId,
                additionalConstraint = { StarWarsFilms.sequelId eq 8 }
            ).selectAll()
            complexInnerJoin.count() shouldBe 1
            complexInnerJoin.forEach {
                it[StarWarsFilms.name] shouldNotBe null
                it[StarWarsFilms.sequelId] shouldBe it[Players.sequelId]
                it[Players.name] shouldBe "Mark Hamill"
            }
        }
    }

    test("when join with alias then fun") {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(StarWarsFilms, Players)
            StarWarsFilms.insert {
                it[name] = "The Last Jedi"
                it[sequelId] = 8
                it[director] = "Rian Johnson"
            }
            StarWarsFilms.insert {
                it[name] = "The Force Awakens"
                it[sequelId] = 7
                it[director] = "J.J. Abrams"
            }
            val sequel = StarWarsFilms.alias("sequel")
            Join(
                table = StarWarsFilms,
                otherTable = sequel,
                additionalConstraint = { sequel[StarWarsFilms.sequelId] eq StarWarsFilms.sequelId + 1 }
            ).selectAll().forEach {
                it[sequel[StarWarsFilms.sequelId]] shouldBe it[StarWarsFilms.sequelId] + 1
            }
        }
    }

    test("delete example") {
        Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        transaction {
            SchemaUtils.create(StarWarsFilms)

            val batchInsertObjects = listOf(
                InsertItem("The Last Jedi", 8, "Rian Johnson"),
                InsertItem("The Force Awakens", 7, "George Lucas"),
                InsertItem("Episode I – The Phantom Menace", 9, "George Lucas"),
            )

            StarWarsFilms.batchInsert(batchInsertObjects) { item ->
                this[StarWarsFilms.name] = item.name
                this[StarWarsFilms.sequelId] = item.sequelId
                this[StarWarsFilms.director] = item.director
            }

            StarWarsFilms.deleteWhere { StarWarsFilms.director eq "George Lucas" }

            StarWarsFilms.selectAll().count() shouldBe 1
        }
    }

    test("when entity then dao") {
        val database = Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        val connection =
            database.connector.invoke() //Keep a connection open so the DB is not destroyed after the first transaction

        val inserted = transaction {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(StarWarsFilms, Players)
            val theLastJedi = StarWarsFilm.new {
                name = "The Last Jedi"
                sequelId = 8
                director = "Rian Johnson"
            }
            TransactionManager.current().entityCache.inserts.isEmpty() shouldBe false
            theLastJedi.id.value shouldBe 1 //Reading this causes a flush
            TransactionManager.current().entityCache.inserts.isEmpty() shouldBe true
            theLastJedi
        }

        transaction {
            val theLastJedi = StarWarsFilm.findById(1)
            theLastJedi shouldNotBe null
            theLastJedi?.id shouldBe inserted.id
        }

        // update field
        transaction {
            val theLastJedi = StarWarsFilm.findById(1)
            theLastJedi?.name = "Episode VIII – The Last Jedi"
        }

        transaction {
            val theLastJedi = StarWarsFilm.findById(1)
            theLastJedi?.name shouldBe "Episode VIII – The Last Jedi"
        }

        // delete object
        transaction {
            val theLastJedi = StarWarsFilm.findById(1)
            theLastJedi shouldNotBe null
            theLastJedi?.delete()
        }

        transaction {
            val theLastJedi = StarWarsFilm.findById(1)
            theLastJedi shouldBe null
        }

        connection.close()
    }

    test("users example") {
        val database = Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        val connection = database.connector.invoke()

        transaction {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(StarWarsFilms, Users, UserRatings)

            val theLastJedi = StarWarsFilm.new {
                name = "The Last Jedi"
                sequelId = 8
                director = "Rian Johnson"
            }

            val someUser = User.new {
                name = "Some User"
            }

            val rating = UserRating.new {
                value = 9
                user = someUser
                film = theLastJedi
            }

            rating.film shouldBe theLastJedi
        }
        connection.close()
    }

    test("when many to one then navigation") {
        val database = Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        val connection = database.connector.invoke()
        transaction {
            addLogger(StdOutSqlLogger)
            SchemaUtils.create(StarWarsFilms, Players, Users, UserRatings)
            val theLastJedi = StarWarsFilm.new {
                name = "The Last Jedi"
                sequelId = 8
                director = "Rian Johnson"
            }
            val someUser = User.new {
                name = "Some User"
            }
            val rating = UserRating.new {
                value = 9
                user = someUser
                film = theLastJedi
            }
            rating.film shouldBe theLastJedi
            rating.user shouldBe someUser
            theLastJedi.ratings.first() shouldBe rating
        }

        transaction {
            val theLastJedi = StarWarsFilm.find { StarWarsFilms.sequelId eq 8 }.first()
            val ratings = UserRating.find { UserRatings.film eq theLastJedi.id }
            ratings.count() shouldBe 1
            val rating = ratings.first()
            rating.user.name shouldBe "Some User"
            theLastJedi.ratings.first() shouldBe rating
            UserRating.new {
                value = 8
                user = rating.user
                film = theLastJedi
            }
            theLastJedi.ratings.count() shouldBe 2
        }
        connection.close()
    }

    test("when many to many then association") {
        val database = Database.connect("jdbc:h2:mem:test", driver = "org.h2.Driver")
        val connection = database.connector.invoke()
        val film = transaction {
            SchemaUtils.create(StarWarsFilms)
            StarWarsFilm.new {
                name = "The Last Jedi"
                sequelId = 8
                director = "Rian Johnson"
            }
        }

        val actor = transaction {
            SchemaUtils.create(Actors)
            Actor.new {
                firstname = "Daisy"
                lastname = "Ridley"
            }
        }

        transaction {
            SchemaUtils.create(StarWarsFilmActors)
            film.actors = SizedCollection(listOf(actor))
        }
        connection.close()
    }

})

class StarWarsFilm(id: EntityID<Int>) : Entity<Int>(id) {
    companion object : EntityClass<Int, StarWarsFilm>(StarWarsFilms)

    var sequelId by StarWarsFilms.sequelId
    var name by StarWarsFilms.name
    var director by StarWarsFilms.director
    var actors by Actor via StarWarsFilmActors
    val ratings by UserRating referrersOn UserRatings.film
}

object Actors : IntIdTable() {
    val firstname = varchar("firstname", 50)
    val lastname = varchar("lastname", 50)
}

class Actor(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<Actor>(Actors)

    var firstname by Actors.firstname
    var lastname by Actors.lastname
}

object StarWarsFilmActors : Table() {
    val starWarsFilm = reference("starWarsFilm", StarWarsFilms).primaryKey(0)
    val actor = reference("actor", Actors).primaryKey(1)
}

class UserRating(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<UserRating>(UserRatings)

    var value by UserRatings.value
    var film by StarWarsFilm referencedOn UserRatings.film
    var user by User referencedOn UserRatings.user
}

object UserRatings : IntIdTable() {
    val value = long("value")
    val film = reference("film", StarWarsFilms)
    val user = reference("user", Users)
}

class User(id: EntityID<Int>) : IntEntity(id) {
    companion object : IntEntityClass<User>(Users)

    var name by Users.name
}

object Users : IntIdTable() {
    val name = varchar("name", 50)
}

object Cities : IntIdTable() {
    val name = varchar("name", 50)
}

object StarWarsFilms : IntIdTable("STAR_WARS_FILMS") {
    val sequelId = integer("sequel_id").uniqueIndex()
    val name = varchar("name", 50)
    val director = varchar("director", 50)
}

object Players : Table() {
    //val sequelId = integer("sequel_id").uniqueIndex().references(StarWarsFilms.sequelId)
    val sequelId = reference("sequel_id", StarWarsFilms.sequelId).uniqueIndex()

    //val filmId = reference("film_id", StarWarsFilms).nullable()
    val name = varchar("name", 50)
}

data class InsertItem(val name: String, val sequelId: Int, val director: String)