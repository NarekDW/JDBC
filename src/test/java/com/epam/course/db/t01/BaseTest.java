package com.epam.course.db.t01;

import org.junit.Test;

import java.sql.Date;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Date: 16.02.2017
 *
 * @author Karapetyan N.K
 */
public class BaseTest {

    @Test
    public void CRUDTest() throws Exception {
        Base  base = new Base("src/test/resources/");
        assertNotNull(base.getConnection());
        assertTrue(base.insertPerson(Date.valueOf("1993-05-13"), "Иван", "Иванов"));
        Person person1  = new Person(1, "Иван", "Иванов", Date.valueOf("1993-05-13"));
        assertThat(base.selectByID(1), is(person1));
        assertTrue(base.getAllFromDb().contains(person1));
        assertTrue(base.updateByID(1, "Иосиф", "Сталин", Date.valueOf("1891-05-17")));
        Person person2  = new Person(1, "Иосиф", "Сталин", Date.valueOf("1891-05-17"));
        assertThat(base.selectByID(1), is(person2));
        assertTrue(base.dropTable("PERSON"));
        assertTrue(base.closeConnection());
    }
}