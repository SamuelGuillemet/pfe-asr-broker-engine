package pfe_broker.daos;

import java.util.List;

import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

public class GenericDAO<T> {
    private static final String HIBERNATE_CONFIG_FILE = "hibernate.cfg.xml";

    private final Class<T> clazz;
    private final SessionFactory sessionFactory;

    public GenericDAO(Class<T> clazz) {
        this.clazz = clazz;
        this.sessionFactory = new Configuration().configure(HIBERNATE_CONFIG_FILE).buildSessionFactory();
    }

    public void closeSessionFactory() {
        sessionFactory.close();
    }

    public void create(T entity) {
        Transaction transaction = null;
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            session.save(entity);
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        }
    }

    public T getById(Long id) {
        try (Session session = sessionFactory.openSession()) {
            return session.get(clazz, id);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public List<T> getAll() {
        try (Session session = sessionFactory.openSession()) {
            CriteriaBuilder builder = session.getCriteriaBuilder();
            CriteriaQuery<T> query = builder.createQuery(clazz);
            Root<T> root = query.from(clazz);
            query.select(root);
            return session.createQuery(query).getResultList();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void update(T entity) {
        Transaction transaction = null;
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            session.update(entity);
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        }
    }

    public void delete(T entity) {
        Transaction transaction = null;
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            session.delete(entity);
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        }
    }

    public void deleteMultiple(List<T> entities) {
        Transaction transaction = null;
        try (Session session = sessionFactory.openSession()) {
            transaction = session.beginTransaction();
            entities.forEach(session::delete);
            transaction.commit();
        } catch (Exception e) {
            if (transaction != null) {
                transaction.rollback();
            }
            e.printStackTrace();
        }
    }

    public List<T> getByAttribute(String attributeName, Object value) throws DAOException {
        if (attributeName == null) {
            throw new DAOException("Attribute name cannot be null");
        }
        try {
            clazz.getDeclaredField(attributeName);
        } catch (NoSuchFieldException e) {
            throw new DAOException(
                    "Attribute " + attributeName + " does not exist in class " + clazz.getName());
        }

        try (Session session = sessionFactory.openSession()) {
            CriteriaBuilder builder = session.getCriteriaBuilder();
            CriteriaQuery<T> query = builder.createQuery(clazz);
            Root<T> root = query.from(clazz);
            query.select(root).where(builder.equal(root.get(attributeName), value));
            return session.createQuery(query).getResultList();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
