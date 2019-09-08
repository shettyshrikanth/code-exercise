package net.bgc.spark;

import java.io.InputStream

import grizzled.slf4j.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.{ Outcome, fixture }
import net.bgc.spark.BgcExcercise._
import java.io.File

class BgcExcerciseTest extends fixture.FunSuite with Logging {

  type FixtureParam = SparkSession

  def withFixture(test: OneArgTest): Outcome = {
    val sparkSession = SparkSession.builder
      .appName("Test-Bgc-Exercise")
      .master("local[*]")
      .getOrCreate()
    try {
      withFixture(test.toNoArgTest(sparkSession))
    } finally sparkSession.stop
  }

  test("Top 20 movies with a minimum of 50 votes with the ranking determined by: (numVotes/averageNumberOfVotes) * averageRating") { spark =>
    val topMovies = get20TopMovies(spark)

    assert(topMovies(0).mkString("\t") === "tt0111161	movie	The Shawshank Redemption	 The Shawshank Redemption	 0	1994	 \\N  142 Drama")
    assert(topMovies(1).mkString("\t") === "tt0468569	movie	The Dark Knight The Dark Knight 0	2008	 \\N  152 Action,Crime,Drama")
    assert(topMovies(2).mkString("\t") === "tt1375666	movie	Inception	Inception	0	2010	\\N  148 Action,Adventure,Sci-Fi")
    assert(topMovies(3).mkString("\t") === "tt0137523	movie	Fight Club  Fight Club  0	1999	\\N  139 Drama")
    assert(topMovies(4).mkString("\t") === "tt0110912	movie	Pulp Fiction	 Pulp Fiction	 0	1994	\\N  154 Crime,Drama")
    assert(topMovies(5).mkString("\t") === "tt0109830	movie	Forrest Gump	 Forrest Gump	 0	1994	\\N  142 Drama,Romance")
    assert(topMovies(6).mkString("\t") === "tt0167260	movie	The Lord of the Rings: The Return of the King	The Lord of the Rings: The Return of the King	0	2003	 \\N  201 Adventure,Drama,Fantasy")
    assert(topMovies(7).mkString("\t") === "tt0120737	movie	The Lord of the Rings: The Fellowship of the Ring	The Lord of the Rings: The Fellowship of the Ring	0	2001	 \\N  178 Adventure,Drama,Fantasy")
    assert(topMovies(8).mkString("\t") === "tt0068646	movie	The Godfather	The Godfather	0	1972	 \\N  175 Crime,Drama")
    assert(topMovies(9).mkString("\t") === "tt0133093	movie	The Matrix  The Matrix  0	1999	 \\N  136 Action,Sci-Fi")
    assert(topMovies(10).mkString("\t") === "tt0167261	movie	The Lord of the Rings: The Two Towers	The Lord of the Rings: The Two Towers	0	2002	 \\N  179 Adventure,Drama,Fantasy")
    assert(topMovies(11).mkString("\t") === "tt1345836	movie	The Dark Knight Rises	The Dark Knight Rises	0	2012	 \\N  164 Action,Thriller")
    assert(topMovies(12).mkString("\t") === "tt0816692	movie	Interstellar	 Interstellar	 0	2014	 \\N  169 Adventure,Drama,Sci-Fi")
    assert(topMovies(13).mkString("\t") === "tt0114369	movie	Se7en	Se7en	0	1995	 \\N  127 Crime,Drama,Mystery")
    assert(topMovies(14).mkString("\t") === "tt0172495	movie	Gladiator	Gladiator	0	2000	 \\N  155 Action,Adventure,Drama")
    assert(topMovies(15).mkString("\t") === "tt1853728	movie	Django Unchained	 Django Unchained	 0	2012	 \\N  165 Drama,Western")
    assert(topMovies(16).mkString("\t") === "tt0102926	movie	The Silence of the Lambs	 The Silence of the Lambs	 0	1991	 \\N  118 Crime,Drama,Thriller")
    assert(topMovies(17).mkString("\t") === "tt0372784	movie	Batman Begins	Batman Begins	0	2005	 \\N  140 Action,Adventure")
    assert(topMovies(18).mkString("\t") === "tt0108052	movie	Schindler's List	 Schindler's List	 0	1993	 \\N  195 Biography,Drama,History")
    assert(topMovies(19).mkString("\t") === "tt0076759	movie	Star Wars: Episode IV - A New Hope  Star Wars	0	1977	 \\N  121 Action,Adventure,Fantasy")
  }

  test("Persons who are most often credited ordered by their credits") { spark =>
    val mostOftenCredited = getMostOftenCreditedPersonsOfTop20Movies(spark)

    assert(mostOftenCredited(0).mkString("\t") === "nm0000151	Morgan Freeman	1937	\\N	actor,producer,soundtrack	tt0114369,tt0097239,tt1057500,tt0405159")
    assert(mostOftenCredited(12).mkString("\t") === "nm0748665	Al Ruddy	1930	\\N	writer,producer,miscellaneous	tt0068646,tt0084316,tt0087032,tt0405159")
    assert(mostOftenCredited(151).mkString("\t") === "nm0138287	Phyllis Carlyle	\\N	\\N	producer,casting_department,casting_director	tt0246072,tt0094606,tt0114369,tt0964539")

    assert(mostOftenCredited.length === 152)
  }

  test("Different titles of the top 20 movies -") { spark =>
    val alias = getDifferentTitlesOfTop20Movies(spark)

    scala.util.Sorting.quickSort(alias)

    assert(alias(0) === "tt0068646	De peetvader,Mario Puzo's The Godfather,Der Pate,Крёстный отец,El padrino,The Godfather,Le parrain,Krstný Otec,Krikštatevis,Gudfadern,A keresztapa,Ristiisa,Godfather,Ojciec chrzestny,O nonos,El Padrino,Kmotr,El padrí,O Poderoso Chefão,Кръстникът,Il padrino,Guðfaðirinn,Daeboo,Goddofâzâ,Boter,Gudfaren,Kumbari,Kum,Pedar khandeh,Хрещений батько,Baba,Hasandak,Al-arraab,Ο νονός,O Padrinho,Bo Gia,Nasul,Kummisetä,Natlimama")
    assert(alias(1) === "tt0076759	O polemos ton astron,La guerra de las estrellas,Star Wars: A New Hope,Star Wars,Zvezdani ratovi: Nova nada,Hviezdne vojny IV: Nová nádej,Star Wars - Episode IV: Eine neue Hoffnung,La guerre des étoiles - Un nouvel éspoir,The Star Wars: From the Adventures of Luke Starkiller,Hviezdne vojny 4: Nová nádej,Star Wars, Episódio IV: Uma Nova Esperança,Star Wars: Episode IV - A New Hope,Hvězdné války IV: Nová naděje,Tähtien sota,Star Wars: Uusi toivo,La guerra de las galaxias,Adventures of the Starkiller: Episode 1 - The Star Wars,Žvaigždžiu karai,Gwiezdne wojny,Ratovi zvijezda IV: Nova nada,Gwiezdne wojny, czesc IV: Nowa nadzieja,Vojna zvezd: Epizoda IV - Novo upanje,Razboiul stelelor,Star Wars IV - Una nuova speranza,Star Wars: Episode IV - Et nytt håp,Stjernekrigen,A Guerra das Estrelas,A New Hope,Sutâ wôzu episoddo 4: Aratanaru kibô,Star Wars: Episodio IV - Una nuova speranza,Star Wars - Stjernekrigen,Războiul Stelelor - Episodul IV: O nouă speranţă,Guerra nas Estrelas,Ο πόλεμος των άστρων,Star Wars: Episod IV - Nytt hopp,The Star Wars,Star wars: Osa IV - Uus lootus,O polemos ton astron: Mia nea elpida,Yildiz Savaslari: Bölüm IV - Yeni Bir Umut,Stjärnornas krig,Звёздные войны. Эпизод 4: Новая надежда,Ratovi zvezda - Nova nada,Star wars: Épisode IV, la guerre des étoiles,Междузвездни войни: Епизод IV - Нова Надежда,Războiul Stelelor: Ediţie specială,Jang-e setarehha,Star Wars IV. rész - Egy új remény,Ratovi zvijezda,Stjörnustríð,The Adventures of Luke Starkiller as Taken from the 'Journal of the Whills': Saga I - Star Wars,Csillagok háborúja,Star Wars: Episodio IV - Una nueva esperanza,Yildiz Savaslari,La guerre des étoiles,Tähtede sõda,Krieg der Sterne,Guerre stellari,Звёздные войны,The War of the World,Зорянi вiйни: Епiзод 4 - Нова надiя,Star Wars: Episodi IV - Uusi toivo,Star Wars: Episódio IV - Uma Nova Esperança")
    assert(alias(2) === "tt0102926	The Silence of the Lambs,Η σιωπή των αμνών,O Silêncio dos Inocentes,När lammen tystnar,Sokout-e bareha,Le silence des agneaux,El silencio de los inocentes,Молчание ягнят,Kad jaganjci utihnu,Ko jagenjčki obmolknejo,El silenci dels anyells,Das Schweigen der Lämmer,Mlčanie jahniat,Kravta dumili,Uhrilampaat,Silence of the Lambs,Nattsvermeren,Voonakeste vaikimine,Kuzularin Sessizligi,Мълчанието на агнетата,Su Im Lang Cua Bay Cuu,Somt alhemlan,I siopi ton amnon,Milczenie owiec,Il silenzio degli innocenti,Mlčení jehňátek,Mlčanie Jahniat,A bárányok hallgatnak,El silencio de los corderos,Hitsujitachi no Chinmoku,De schreeuw van het lam,Tacerea mieilor,Avineliu tylejimas,Ondskabens øjne,Мовчання ягнят")
    assert(alias(3) === "tt0108052	Schindler's List,Fehrest-e Schindler,Steven Spielberg's Schindler's List,Schindlerův seznam,La liste de Schindler,La llista de Schindler,Shindorâ no risuto,La lista de Schindler,Schindlerjev seznam,Schindlers Liste,Шиндлеровата Листа,A Lista de Schindler,Šindlerov Zoznam,Shindleris sia,Ban danh sách cua Schindler,Schindler'in Listesi,Списъкът на Шиндлер,Schindlers lista,Schindlerova lista,Schindlers liste,Šindlerio sąrašas,Η λίστα του Σίντλερ,Список Шиндлера,Schindleri nimekiri,Schindler listája,I lista tou Schindler,Šindlerova lista,Lista Schindlera,Lista lui Schindler,Shindlerin Siyahisi,Schindlerin lista,Schindler's List - La lista di Schindler,Reshimut Schindler")
    assert(alias(4) === "tt0109830	Forrest Gump,Φόρεστ Γκαμπ,Форест Гъмп,Forest Gamp,Форрест Гамп,Forrest Qamp,Forrest Gump: The IMAX Experience,Forestas Gampas,Forrest Gump: O Contador de Histórias")
    assert(alias(5) === "tt0110912	Tiempos violentos,Dastan-e aame pasand,Pulp Fiction,Кримiнальне чтиво,Makulatura,Криминале,Petparačke priče,Šund,Tarinoita väkivallasta,Pulp Fiction: Historky z podsvetia,Pulp Fiction: Historky z podsvětí,Black Mask,Fiction pulpeuse,Lubene,Ponyvaregény,Pulp Fiction: Tempo de Violência,Pulp Fiction: Tiempos violentos,Pulp Fiction: Tarinoita väkivallasta,Bulvarinis skaitalas,Ucuz Roman,Kriminal Qirayet,Sifrut Zolla,Криминальное чтиво,Pakleni šund")
    assert(alias(6) === "tt0111161	Rastegari dar Shawshank,Avain pakoon,Os Condenados de Shawshank,The Shawshank Redemption,Sueños de Libertad,Sueño de fuga,Sueños de libertad,Rita Hayworth - Avain pakoon,Um Sonho de Liberdade,A remény rabjai,Bekstvo iz Šošenka,Die Verurteilten,Le ali della libertà,Nyckeln till frihet,Vykoupení z věznice Shawshank,Rita Hayworth - nyckel till flykten,Rita Hayworth - avain pakoon,Τελευταία έξοδος: Ρίτα Χέιγουορθ,Shôshanku no sora ni,Kaznilnica odrešitve,Sueño de libertad,De ontsnapping,Изкуплението Шоушенк,En verden udenfor,Xiao shen ke de jiu shu,Homot Shel Tikva,Iskupljenje u Shawshanku,Rita Hayworth -Avain pakoon,Pabegimas iš Šoušenko,Frihetens regn,Shawshanki lunastus,Skazani na Shawshank,Nhà tù Shawshank,Esaretin Bedeli,Închisoarea îngerilor,Teleftaia exodos: 'Rita Hayworth',Cadena perpètua,Les évadés,À l'ombre de Shawshank,Vykúpenie z väznice Shawshank,Cadena perpetua,The Prisoner,Sueños de fuga,Побег из Шоушенка,Shawshank Redemption - Avain pakoon,Gaqceva shoushenkidan,Втеча з Шоушенка,Shoushenkden Qacish,Rita Hayworth and Shawshank Redemption")
    assert(alias(7) === "tt0114369	Shvidi,Sedem,Sebun,Siedem,Seven,Seven: Os Sete Crimes Capitais,Sept,Epta,Pecados capitales,Haft,Se7en,Seven - 7 Pecados Mortais,Семь,The Seven Deadly Sins,Sedam,Сiм,Seven, los siete pecados capitales,Septyni,Seitsemän,Sieben,Седем,Sete Pecados Mortais,Yedi,Yeddi,Hetedik,Seitse,Se7en: pecados capitales,Sedm,Sep7,Los siete pecados capitales")
    assert(alias(8) === "tt0120737	Le seigneur des anneaux - La communauté de l'anneau,Stapînul inelelor: Fratia inelului,Властелин колец: Братство кольца,Il Signore degli Anelli - La compagnia dell'Anello,Pán prsteňov: Spoločenstvo Prsteňa,Sõrmuste isand: Sõrmuse vennaskond,Stapânul Inelelor: Fratia Inelului,Sagan om ringen: Härskarringen,Yüzüklerin Efendisi: Yüzük Kardesligi,Ringenes herre: Eventyret om ringen,Hringadróttinssaga: Föruneyti hringsins,The Lord of the Rings: The Fellowship of the Ring,El señor de los anillos: La comunidad del anillo,Lord of the Rings,Gospodarot na prstenite: Druzhinata na prstenot,In de ban van de ring: De reisgenoten,O arhontas ton dahtylidion: I syntrofia tou dahtylidiou,Władca pierścieni: Drużyna Pierścienia,A Gyűrűk Ura: A gyűrű szövetsége,Taru sormusten herrasta: Sormuksen ritarit,Žiedu Valdovas: Žiedo brolija,Le seigneur des anneaux: La communauté de l'anneau,Arbab-e halgheha: Yaran-e halgheh,Rôdo obu za ringu,O Senhor dos Anéis - A Irmandade do Anel,Ringenes herre: Ringens brorskap,O Senhor dos Anéis: A Sociedade do Anel,Gospodar prstenova: Prstenova družina,Seyyed alkhavatem: Sohbat alkhatam,The Fellowship of the Ring,Sagan om ringen,Pán prstenů: Společenstvo prstenu,El senyor dels anells: La germandat de l'anell,Властелинът на пръстените: Задругата на пръстена,Bechdebis mbrdzanebeli: Bechdis sadzmo,Sar HaTabaot: Achvat HaTabaat,Gospodar prstenova - Družina prstena,El senyor dels anells: La comunitat de l'anell,Володар перснiв: Хранителi персня,Der Herr der Ringe: Die Gefährten,Ο άρχοντας των δαχτυλιδιών: Η συντροφιά του δαχτυλιδιού,Kryezoti i Unazave: Besëlidhja e Unazës,The Lord of the Rings: The Fellowship of the Ring: The Motion Picture,Gospodar prstanov: Bratovscina prstana")
    assert(alias(9) === "tt0133093	Matrix,Matrikss,Matrica,Matriks,The Matrix,Matorikkusu,La matriz,Maatriks,Fylkið,Матрицата,Матрикс,La matrice,Mátrix,Матриця,Матрица")
    assert(alias(10) === "tt0137523	Fight club - Sala de lupte,Бойцовский клуб,Fight Club,Harcosok klubja,Döyüshçü Klubu,Clube da Luta,El club de la pelea,Бiйцiвський клуб,Klub golih pesti,Боен клуб,Klub boraca,Nadi alghetal,Kaklusklubi,Podziemny krąg,Kovos klubas,Mo'adon Krav,Borechki Klub,El club de la lucha,Klub Bitkárov,Bashgah-e moshtzani,Clube de Combate,Klub rváčů,Borilački klub,Dövüs Kulübü")
    assert(alias(11) === "tt0167260	Seyed alkhavatem 3: Awdat almalek,O Senhor dos Anéis - O Regresso do Rei,El senyor dels anells: El retorn del rei,Властелин колец: Возвращение короля,Le seigneur des anneaux: Le retour du roi,Ο άρχοντας των δαχτυλιδιών: Η επιστροφή του βασιλιά,The Lord of the Rings: The Return of the King,Taru sormusten herrasta: Kuninkaan paluu,Der Herr der Ringe: Die Rückkehr des Königs,El señor de los anillos: El retorno del rey,Arbab-e halgheha 3: Bazgasht-e padeshah,Ringenes herre: Kongen vender tilbage,Володар перснiв: Повернення короля,Gospodar prstenova - Povratak kralja,Pán prsteňov: Návrat kráľa,O arhontas ton dahtylidion: I epistrofi tou vasilia,In de ban van de ring: De terugkeer van de koning,Ringenes herre: Atter en konge,Stapanul Inelelor: Intoarcerea Regelui,Kryezoti i Unazave: Kthimi i Mbretit,Властелинът на пръстените: Завръщането на краля,Gospodar prstenova: Povratak kralja,Pán prstenů: Návrat krále,A Gyűrűk Ura: A király visszatér,Il Signore degli Anelli - Il ritorno del re,The Return of the King,Taru Sormusten Herrasta - Kuninkaan paluu,Rôdo obu za ringu - Ô no kikan,Gospodarot na prstenite: Vrakjanjeto na kralot,O Senhor dos Anéis: O Retorno do Rei,Hringadróttinssaga: Hilmir snýr heim,Sõrmuste isand: Kuninga tagasitulek,Le seigneur des anneaux - Le retour du roi,Žiedu Valdovas: Karaliaus sugrižimas,El señor de los anillos - El retorno del rey,Sagan om konungens återkomst,Yüzüklerin Efendisi: Kralin Dönüsü,Władca pierścieni: Powrót króla,Sar Hatabaot: Shivat Hamelekh',Gospodar prstanov: Kraljeva vrnitev")
    assert(alias(12) === "tt0167261	Arbab-e halgheha 2: Do borj,Володар перснiв: Двi вежi,The Lord of the Rings: The Two Towers,Ringenes herre: De to tårne,Le seigneur des anneaux: Les deux tours,Sõrmuste isand: Kaks kantsi,O Senhor dos Anéis - As Duas Torres,A Gyűrűk Ura: A két torony,Hringadróttinssaga: Tveggja turna tal,Rôdo obu za ringu - Futatsu no tô,Stapânul inelelor: Cele doua turnuri,Władca pierścieni: Dwie wieże,Sagan om de två tornen,Pán prstenů: Dvě věže,El señor de los anillos: Las dos torres,In de ban van de ring: De twee torens,O arhontas ton dahtylidion: Oi dyo pyrgoi,Ο άρχοντας των δαχτυλιδιών: Οι δύο πύργοι,Gospodar prstanov: stolpa,O Senhor dos Anéis: As Duas Torres,Taru sormusten herrasta: Kaksi tornia,The Two Towers,Властелин колец: Две крепости,Ringenes herre: To tårn,Sagan om de två tornen - härskarringen,Властелинът на пръстените: Двете кули,Il Signore degli Anelli - Le due torri,Banjieui jewang du gaeeui tab,Le seigneur des anneaux - Les deux tours,Gospodar prstenova - Dve kule,Sar Hatabaot: Shnei Hatzri'kh'im,Yüzüklerin Efendisi: Iki Kule,Gospodarot na prstenite: Dvete kuli,Kryezoti i Unazave: Dy Kullat,Der Herr der Ringe: Die zwei Türme,Seyed alkhavatem 2,Pán prsteňov: Dve veže,Žiedu Valdovas: Dvi tvirtoves,Gospodar prstenova: Dvije kule,El senyor dels anells: Les dues torres")
    assert(alias(13) === "tt0172495	Gladiator,Vijeta,Гладiатор,Gladiateur,Gladiador,Skylmingaþrællinn,Gladiatorul,Μονομάχος,Gladyatör,Monomahos,Гладиатор,Gladiator - Special Edition,Gladijator,The Gladiators,Gladiaattori,Gladiaator,Gladiatoren,Gladiátor,Gladiatorius,Gladiator (El gladiador),Il gladiatore")
    assert(alias(14) === "tt0372784	Batman: na začetku,Бетмен: Початок,Batman - El inicio,Batman Inicia,Batman Begins: The IMAX Experience,Batman - Początek,Batman: El comienzo,Batman Begins,Batman inicia,Μπάτμαν: Η αρχή,Batman Basliyor,Batman - Inceputuri,Batman 5,Batman comienza,Batman začíná,Batman - O Início,Бэтмен: Начало,Betmen počinje,Batman: Kezdődik!,Netopierí muž začína,Betmenas: Pradžia,Party In Fresno,Батман в началото,Batman: Početak,Batman: Le commencement,Batman začína,Batman alustab,The Intimidation Game,Batman: Intimidation")
    assert(alias(15) === "tt0468569	The Dark Knight,Temný Rytier,Vitez tame,Yön ritari,Ο σκοτεινός ιππότης,Batman - El caballero de la noche,Ky si bóng dêm,Untitled Batman Begins Sequel,Kara Sövalye,Cavalerul negru,Temný rytíř,Bianfu xia - Heiye zhi shen,Тёмный рыцарь,Темний лицар,Winter Green,Batman: O Cavaleiro das Trevas,Iravu Kavalan,Черният рицар,The Dark Knight: Le chevalier noir,Batman Begins 2,Kalorësi i Errësirës,Le chevalier noir,O Cavaleiro das Trevas,Mračni vitez,Pimeduse rüütel,Mroczny rycerz,El caballero oscuro,Shavi Raindi,A sötét lovag,O skoteinos ippotis,Vitez teme,Tamsos riteris,Dâku naito,Batman: Atsawinrattikan,Batman 2 - El caballero de la noche,El cavaller fosc,Batman: El Caballero de la Noche,The Dark Knight: The IMAX Experience,Rökkurriddarinn,Batman: El caballero de la noche,Il cavaliere oscuro,Rory's First Kiss,Batman: The Dark Knight")
    assert(alias(16) === "tt0816692	Yildizlararasi,Interstellar: Calatorind prin univers,Iнтeрстеллар,Udhëtimi Ndëryjor,Interstellar,Du Hành Liên Sao,Interstelari,Interestelar,Flora's Letter,Xing Ji Chuan Yue,Интерстеллар,Interstellaire,Untitled Steven Spielberg Space Project,Интерстелар,Csillagok között,Medzvezdje,Tähtedevaheline,Bein kokhavim,Međuzvezdani,Ulduzlararasi,Tarp žvaigždžiu")
    assert(alias(17) === "tt1345836	Vzpon Viteza teme,Tamsos riterio sugrižimas,Batman: El caballero de la noche asciende,Dâku naito raijingu,Arkham,Tumsais bruninieks atgriezas,The Dark Knight Rises,Gotham,Návrat Temného rytiera,Batman 3,Черният рицар: Възраждане,Uspon Mračnog viteza,A sötét lovag: Felemelkedés,Ky Si Bong Dem Troi Day,Shavi Raindis Agzeveba,O Cavaleiro das Trevas Renasce,El caballero oscuro: La leyenda renace,Vitez tame: Povratak,The Dark Knight Rises: The IMAX Experience,Kara Sövalye Yükseliyor,L'ascension du chevalier noir,Bianfu xia: Ye shen qiyi,Dakeu naiteu raijeu,Pinfuk haap: Ye san heiyi,Bianfu xia: Hei'an qishi jueqi,Ο σκοτεινός ιππότης: Η επιστροφή,O skoteinos ippotis: I epistrofi,Pimeduse rüütli taastulek,Aliyato shel ha'abir ha'af'el,Mroczny Rycerz powstaje,Hei'an qishi: Liming shengqi,Batman: O Cavaleiro das Trevas Ressurge,Тёмный рыцарь: Возрождение легенды,Batman 3: Dark knight ki vijay,T.D.K.R.,Yön Ritarin paluu,El cavaller fosc: La llegenda reneix,Cavalerul negru: Legenda renaste,Темний лицар повертаеться,Temný rytíř povstal,Il cavaliere oscuro - Il ritorno,Magnus Rex")
    assert(alias(18) === "tt1375666	Începutul,Початок,Algus,Početak,Chakravuyh,Počátek,Οι ονειροπαγιδευτές,Eredet,Hat'hala,El origen,Inception,Генезис,Baslangiç,Начало,Izvor,Jit Phi Khat Lôk,Pirmsakums,Dasatskisi,Oliver's Arrow,Počiatok,Origen,A Origem,Talghin,Pradžia,Inception: The IMAX Experience,Incepcja,Origine")
    assert(alias(19) === "tt1853728	Zencirsiz Canqo,Django Unchained,Django Libertado,Django, ο τιμωρός,Django elszabadul,Django sin cadenas,Odbjegli Django,Nespoutaný Django,Vabastatud Django,Джанго освобожденный,Jango-ye raha shode,Django Khot Khon Dan Theuoen,Django déchaîné,Đangova osveta,Django, o timoros,Django desencadenat,Джанго без окови,Django desencadenado,Джанго вiльний,Django Lelo Ma'atzorim,Django Livre,Zincirsiz,Django,Django Tsunagarezaru Mono,Ištrukęs Džango,Django dezlantuit,Django brez okovov,Hành Trình Django")
    assert(alias.length === 20)
  }

  private def get20TopMovies(spark: SparkSession) = {
    val ratingsRdd = spark.sparkContext.textFile(getFilePath("src/test/resources/title.ratings.tsv"), 20)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter };

    val ratings = getRatings(ratingsRdd, minimumOfFiftyVotesFilter)

    val moviesRdd = spark.sparkContext.textFile(getFilePath("src/test/resources/title.basics.tsv"), 20)

    val movies = getMovies(moviesRdd, movieTypeFilter)

    getTopMovies(movies, ratings, 20)

  }

  private def getMostOftenCreditedPersonsOfTop20Movies(spark: SparkSession) = {
    val top20movies = get20TopMovies(spark)

    val namesRdd = spark.sparkContext.textFile(getFilePath("src/test/resources/name.basics.tsv"), 20)

    val principalsRdd = spark.sparkContext.textFile(getFilePath("src/test/resources/title.principals.tsv"), 20)

    getMostOftenCredited(principalsRdd, namesRdd, top20movies)
  }

  private def getDifferentTitlesOfTop20Movies(spark: SparkSession) = {
    val top20movies = get20TopMovies(spark)

    val aliasRDD = spark.sparkContext.textFile(getFilePath("src/test/resources/title.akas.tsv"), 1)

    getAliasOfTopMovietitles(aliasRDD, top20movies);
  }

  private def getFilePath(name: String): String = {
    new File(name).getAbsolutePath
  }

  private def getInputData(name: String): Seq[String] = {
    val is: InputStream = getClass.getResourceAsStream(name)
    scala.io.Source.fromInputStream(is).getLines.toSeq
  }
}
