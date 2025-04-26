package be.kuleuven;

import java.sql.Connection;
import java.util.List;

import org.jdbi.v3.core.Jdbi;

public class SpelerRepositoryJDBIimpl implements SpelerRepository {
  private final Jdbi jdbi;

  // Constructor
  public SpelerRepositoryJDBIimpl(Connection connection) {
    jdbi = Jdbi.create(connection);
  }

  public Jdbi getJdbi() {
    return jdbi;
  }

  @Override
  public void addSpelerToDb(Speler speler) {
    try {
      jdbi.withHandle(handle -> {
        return handle.createUpdate("INSERT INTO speler (tennisvlaanderenId, naam, punten) VALUES (:tennisvlaanderenId, :naam, :punten)")
                    .bind("tennisvlaanderenId", speler.getTennisvlaanderenId())
                    .bind("naam", speler.getNaam())
                    .bind("punten", speler.getPunten())
                    .execute();
      });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Speler getSpelerByTennisvlaanderenId(int tennisvlaanderenId) {
    try {
    return jdbi.withHandle(handle -> 
        handle.createQuery("SELECT * FROM speler WHERE tennisvlaanderenId = :id")
              .bind("id", tennisvlaanderenId)
              .mapToBean(Speler.class)
              .first()
    );
    } catch (Exception e){
      throw new InvalidSpelerException(tennisvlaanderenId + "");
    }
    
  }

  @Override
  public List<Speler> getAllSpelers() {
      try {
          return jdbi.withHandle(handle ->
              handle.createQuery("SELECT * FROM speler")
                    .map((rs, ctx) -> new Speler(
                        rs.getInt("tennisvlaanderenid"),
                        rs.getString("naam"),
                        rs.getInt("punten")))
                    .list()
          );
      } catch (Exception e) {
          throw new RuntimeException("Fout bij ophalen alle spelers", e);
      }
  }


  @Override
  public void updateSpelerInDb(Speler speler) {
      int affectedRows = jdbi.withHandle(handle -> {
          return handle.createUpdate("UPDATE speler SET naam = :naam, punten = :punten WHERE tennisvlaanderenId = :tennisvlaanderenId;")
                       .bind("naam", speler.getNaam())
                       .bind("punten", speler.getPunten())
                       .bind("tennisvlaanderenId", speler.getTennisvlaanderenId())
                       .execute();
      });
  
      if (affectedRows == 0) {
          throw new InvalidSpelerException(speler.getTennisvlaanderenId() + "");
      }
  }
  
  

  @Override
  public void deleteSpelerInDb(int tennisvlaanderenId) {
    int affectedRows = jdbi.withHandle(handle -> {
      return handle.createUpdate("DELETE FROM speler WHERE tennisvlaanderenId = :id")
                   .bind("id", tennisvlaanderenId)
                   .execute();
    });
    if (affectedRows == 0) {
      throw new InvalidSpelerException(tennisvlaanderenId + "");
    }
  }

  @Override
  public String getHoogsteRankingVanSpeler(int tennisvlaanderenId) {
    Speler foundSpeler = jdbi.withHandle(handle -> 
        handle.createQuery("SELECT * FROM speler WHERE tennisvlaanderenId = :id")
              .bind("id", tennisvlaanderenId)
              .mapToBean(Speler.class)
              .first()
    );

    if (foundSpeler == null) {
      throw new InvalidSpelerException(tennisvlaanderenId + "");
    }

    return jdbi.withHandle(handle -> {
      String query = "SELECT t.clubnaam, w.finale, w.winnaar " +
                     "FROM wedstrijd w " +
                     "JOIN tornooi t ON w.tornooi = t.id " +
                     "WHERE (w.speler1 = :id OR w.speler2 = :id) AND w.finale IS NOT NULL " +
                     "ORDER BY w.finale " +
                     "LIMIT 1";

                     return handle.createQuery(query)
                     .bind("id", tennisvlaanderenId)
                     .map((rs, ctx) -> {
                         String tornooinaam = rs.getString("clubnaam");
                         int finale = rs.getInt("finale");
                         int winnaar = rs.getInt("winnaar");
                 
                         String finaleString;
                         switch (finale) {
                             case 1:
                                 finaleString = (winnaar == tennisvlaanderenId) ? "winst" : "finale";
                                 break;
                             case 2:
                                 finaleString = "halve finale";
                                 break;
                             case 4:
                                 finaleString = "kwart finale";
                                 break;
                             default:
                                 finaleString = "plaats " + finale;
                                 break;
                         }
                 
                         return "Hoogst geplaatst in het tornooi van " + tornooinaam + " met plaats in de " + finaleString;
                     })
                     .findFirst()
                     .orElse("Geen ranking gevonden voor speler met ID " + tennisvlaanderenId);
    });
  }

  @Override
  public void addSpelerToTornooi(int tornooiId, int tennisvlaanderenId) {
      jdbi.useHandle(handle -> {
          handle.createUpdate("INSERT INTO speler_speelt_tornooi (speler, tornooi) VALUES (:speler, :tornooi)")
                .bind("speler", tennisvlaanderenId)
                .bind("tornooi", tornooiId)
                .execute();
          handle.commit();
      });
  }

  @Override
  public void removeSpelerFromTornooi(int tornooiId, int tennisvlaanderenId) {
    // Verwijder de speler van het tornooi
    try {
      jdbi.withHandle(handle -> {
        handle.createUpdate("DELETE FROM speler_speelt_tornooi WHERE speler = :spelerId AND tornooi = :tornooiId")
              .bind("spelerId", tennisvlaanderenId)
              .bind("tornooiId", tornooiId)
              .execute();
        return null;
      });
    } catch (Exception e) {
      throw new RuntimeException("Failed to remove speler from tornooi", e);
    }
  }
}
