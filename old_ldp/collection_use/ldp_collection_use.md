```python
import os
import sql
import psycopg2
%load_ext sql
PW = os.environ.get("LDPPW")
```


```python
%sql postgresql://ldp:$PW@glintcore.net/folio_marc_test
```

### This first query looks at all loans (from the circulation_loans table) in a date range (2017-01-01 <= loan_date < 2022-01-01) and counts the number of loans and renewals of each item.


```python
%%sql

DROP TABLE IF EXISTS local.nassibnassar_loan_count;

CREATE TABLE local.nassibnassar_loan_count AS
    SELECT
        item_id,
        coalesce(count(id), 0) AS loan_count,
        coalesce(sum(renewal_count), 0) AS renewal_count
    FROM
        circulation_loans
    WHERE
        loan_date >= '2017-01-01' AND loan_date < '2022-01-01'
    GROUP BY
        item_id;

SELECT * FROM local.nassibnassar_loan_count LIMIT 10;
```

     * postgresql://ldp:***@glintcore.net/folio_marc_test
    Done.
    20741 rows affected.
    10 rows affected.





<table>
    <tr>
        <th>item_id</th>
        <th>loan_count</th>
        <th>renewal_count</th>
    </tr>
    <tr>
        <td>00001fd7-47f8-591e-8fa0-bbf6b08c7f32</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>000174ef-00c4-5ef4-bacd-03a7c0896034</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>0006db63-2905-54da-92e5-8836e637a02b</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>000f1eea-c612-53d1-b2f9-80f832f544ec</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>00131a21-4548-5af1-b233-3a2e1d4fe552</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>00155128-b8a7-591d-9923-da717eaf3bca</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>0015c375-6ca8-599a-a8dd-2b17d7c7328a</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>0016465e-be7b-51ce-a790-07f0cdcc04e1</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>00177e86-5538-5737-af6e-ab9f3f3ba91c</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>001d7405-b4dd-54af-8e19-270edba9753f</td>
        <td>1</td>
        <td>1</td>
    </tr>
</table>



### Since some items may never have been loaned at all, those items would not be included in circulation_loans and therefore would be missing from the previous result.  We can get a list of all of the items (from inventory_items) and join the items to the results from our previous query (local.loan_count).  (By "joining" items, we mean using another table to retrieve additional data that are looked up by the item ID.)  We use the coalesce() function to set 0 as a default count for items that have no loan data.


```python
%%sql

DROP TABLE IF EXISTS local.nassibnassar_item_loan_count;

CREATE TABLE local.nassibnassar_item_loan_count AS
    SELECT
        inventory_items.id AS item_id,
        coalesce(loan_count, 0) AS loan_count,
        coalesce(renewal_count, 0) AS renewal_count
    FROM
        inventory_items
        LEFT JOIN local.nassibnassar_loan_count AS loan_count ON inventory_items.id = loan_count.item_id;

SELECT * FROM local.nassibnassar_item_loan_count LIMIT 10;
```

     * postgresql://ldp:***@glintcore.net/folio_marc_test
    Done.
    110026 rows affected.
    10 rows affected.





<table>
    <tr>
        <th>item_id</th>
        <th>loan_count</th>
        <th>renewal_count</th>
    </tr>
    <tr>
        <td>00001fd7-47f8-591e-8fa0-bbf6b08c7f32</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>0000435d-b5c7-5f8d-b305-1a20a5b94d99</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>00005249-9f67-5961-a664-f6ffc29ace85</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>000174ef-00c4-5ef4-bacd-03a7c0896034</td>
        <td>1</td>
        <td>0</td>
    </tr>
    <tr>
        <td>00017b8d-271a-5bb0-86f1-73069ec62b26</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>0001ebc1-68a9-5da9-bd98-cefc08b70cde</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>00022b2a-1ac7-502b-aca4-dfd669a6657c</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>00023f10-58b3-52a6-9fa5-f93b3d8d1ff7</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>00029e76-16bd-5fdc-9df1-e32e535554e1</td>
        <td>0</td>
        <td>0</td>
    </tr>
    <tr>
        <td>0002c3aa-6255-5225-bd63-416352ec5bce</td>
        <td>0</td>
        <td>0</td>
    </tr>
</table>



### So far we have only retrieved item IDs.  There are a lot of data associated with items, and we can retrieve them by joining our previous results to other tables.  For example, we can use the tables, items_holdings_instances and item_ext.  (These are helper tables that we call "derived tables" because they are generated from the FOLIO source data.)  In this query we will also use a WHERE clause to filter the results on "book" which is simply a way of limiting our results to only print materials.


```python
%%sql

DROP TABLE IF EXISTS local.nassibnassar_item_loan_count_detail;

CREATE TABLE local.nassibnassar_item_loan_count_detail AS
    SELECT
        i.item_id,
        i.loan_count,
        i.renewal_count,
        h.barcode,
        h.holdings_record_id,
        h.hrid,
        h.call_number_type_id,
        h.call_number_type_name,
        h.material_type_id,
        h.material_type_name,
        h.holdings_id,
        h.call_number,
        h.instance_id,
        h.title,
        h.loan_type_id,
        h.loan_type_name,
        e.effective_location_id,
        e.effective_location_name,
        e.status_name
    FROM
        local.nassibnassar_item_loan_count AS i
        LEFT JOIN folio_reporting.items_holdings_instances AS h
            ON i.item_id = h.item_id
        LEFT JOIN folio_reporting.item_ext AS e
            ON e.item_id = i.item_id
    WHERE
        h.material_type_name = 'book';

SELECT * FROM local.nassibnassar_item_loan_count_detail LIMIT 10;
```

     * postgresql://ldp:***@glintcore.net/folio_marc_test
    Done.
    110019 rows affected.
    10 rows affected.





<table>
    <tr>
        <th>item_id</th>
        <th>loan_count</th>
        <th>renewal_count</th>
        <th>barcode</th>
        <th>holdings_record_id</th>
        <th>hrid</th>
        <th>call_number_type_id</th>
        <th>call_number_type_name</th>
        <th>material_type_id</th>
        <th>material_type_name</th>
        <th>holdings_id</th>
        <th>call_number</th>
        <th>instance_id</th>
        <th>title</th>
        <th>loan_type_id</th>
        <th>loan_type_name</th>
        <th>effective_location_id</th>
        <th>effective_location_name</th>
        <th>status_name</th>
    </tr>
    <tr>
        <td>00107f81-1cc0-5f24-a351-829caba65c86</td>
        <td>0</td>
        <td>0</td>
        <td>2000000064468</td>
        <td>f469ef67-665b-54c9-969a-914cb23f5c8b</td>
        <td>it00000064620</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>f469ef67-665b-54c9-969a-914cb23f5c8b</td>
        <td>020 780</td>
        <td>a8fa40d0-8fda-32fb-86d4-509a8df2eaaa</td>
        <td>Musikbibliothek aktuell Mitteilungsblatt der Subkommission &quot;Informations- und Kommunikationsdienste&quot; in der AIBM Arbeitsstelle für das Bibliothekswesen</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Available</td>
    </tr>
    <tr>
        <td>001550f3-51aa-5081-a824-c7ead939963a</td>
        <td>0</td>
        <td>0</td>
        <td>2000000107212</td>
        <td>9e837c35-0b6f-5770-92d1-ae7dd34da64a</td>
        <td>it00000107357</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>9e837c35-0b6f-5770-92d1-ae7dd34da64a</td>
        <td>212 [Ke</td>
        <td>4cae0c9d-9b9b-3ba1-bcf1-eb71f58ce918</td>
        <td>[Kein Hauptsachtitel erfasst]</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>53cf956f-c1df-410b-8bea-27f712cca7c0</td>
        <td>Annex</td>
        <td>Available</td>
    </tr>
    <tr>
        <td>001c953e-a568-5001-b881-e493c29031f2</td>
        <td>0</td>
        <td>0</td>
        <td>2000000097325</td>
        <td>6932daf3-fdeb-552b-9f87-018e325919d1</td>
        <td>it00000097476</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>6932daf3-fdeb-552b-9f87-018e325919d1</td>
        <td>325 Sam</td>
        <td>6c3c9945-e7c7-3daf-9aac-afe928f464a3</td>
        <td>Sampladelic relics &amp; [and] dancefloor oddities deee-remixes [all songs written and produced by Deee-lite]</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Available</td>
    </tr>
    <tr>
        <td>0020ea55-5f58-576a-9dc9-c9e6b11dec5c</td>
        <td>0</td>
        <td>0</td>
        <td>2000000022232</td>
        <td>cc070c55-d8e5-57c4-afa5-bb440134f63b</td>
        <td>it00000022377</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>cc070c55-d8e5-57c4-afa5-bb440134f63b</td>
        <td>232 Hei</td>
        <td>727c9cac-3c8c-334d-9782-d77b5bbdebe3</td>
        <td>Heimatkalender für München und Umland hrsg. von Hans Sponholz</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Available</td>
    </tr>
    <tr>
        <td>00259804-e3a6-52c2-978e-f944863a7bea</td>
        <td>1</td>
        <td>0</td>
        <td>2000000011158</td>
        <td>9e51b2b8-c2c7-58b5-89af-2185ff3f040e</td>
        <td>it00000011300</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>9e51b2b8-c2c7-58b5-89af-2185ff3f040e</td>
        <td>158 Die</td>
        <td>55da4e78-307d-38f6-b51b-c60bf052312b</td>
        <td>Die Einkehr H. 26. Die Vogelfalle / Walter Oelschner</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Checked out</td>
    </tr>
    <tr>
        <td>0037ebdf-f011-5558-8ebd-d946316a7417</td>
        <td>0</td>
        <td>0</td>
        <td>2000000053118</td>
        <td>c39021b8-f725-55e9-9e71-0a38bdf30051</td>
        <td>it00000053266</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>c39021b8-f725-55e9-9e71-0a38bdf30051</td>
        <td>118 La </td>
        <td>4f521ff9-66ea-3620-98bf-af787969009c</td>
        <td>La petite Christine valse musette Musik: J. Löchter ; M. Sauer. Bearb.: F. Knittel</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Available</td>
    </tr>
    <tr>
        <td>00768bca-29d6-558f-aa6c-6de9a4058389</td>
        <td>0</td>
        <td>0</td>
        <td>2000000036102</td>
        <td>2f2785ab-64d9-58a5-91d9-86e709c8324a</td>
        <td>it00000036251</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>2f2785ab-64d9-58a5-91d9-86e709c8324a</td>
        <td>102 Sch</td>
        <td>76836914-1ce6-3690-ad15-d0010b5952c6</td>
        <td>Schweißtechnische Informationen aus dem Zentralinstitut für Schweißtechnik der DDR M 748-84 Bildauswertung mit Mikrorechner / Bearb.: R. Nitzsche; N. Meyendorf</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Available</td>
    </tr>
    <tr>
        <td>009e3fb0-9a4c-5f80-a8f5-b4d21bc9511d</td>
        <td>0</td>
        <td>0</td>
        <td>2000000024741</td>
        <td>1c93bc6b-96bd-5f00-aa44-4d85e99ca7ff</td>
        <td>it00000024890</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>1c93bc6b-96bd-5f00-aa44-4d85e99ca7ff</td>
        <td>741 Sch</td>
        <td>67f46e65-3e8c-3444-a79e-d02486f7a15e</td>
        <td>Schicksals-Roman</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Available</td>
    </tr>
    <tr>
        <td>00a01a1a-e289-5492-9554-e5fa784c824a</td>
        <td>0</td>
        <td>0</td>
        <td>2000000035727</td>
        <td>c50f0be6-ae5b-5c50-a165-f6c9d8e52138</td>
        <td>it00000035878</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>c50f0be6-ae5b-5c50-a165-f6c9d8e52138</td>
        <td>727 Dre</td>
        <td>6ca24561-2e7c-3216-97e1-902146866efa</td>
        <td>Drei Jahrzehnte Denkmalpflege in der DDR überarb. Protokoll e. Konferenz d. Präsidiums d. Kulturbundes d. DDR, d. Zentralvorstandes d. Ges. für Denkmalpflege im Kulturbund d. DDR u. d. Rates für Denkmalpflege beim Ministerium für Kultur am 8.11.1979 Kulturbund d. DDR, Ges. für Denkmalpflege. [Hrsg.: Zentralvorstand d. Ges. für Denkmalpflege im Kulturbund d. DDR]</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Available</td>
    </tr>
    <tr>
        <td>00ac4b49-93ba-5bd4-ace5-c648e789e663</td>
        <td>0</td>
        <td>0</td>
        <td>2000000017808</td>
        <td>ef3e6dbc-4b08-56fc-99c9-cf22df8ab042</td>
        <td>it00000017953</td>
        <td>6caca63e-5651-4db6-9247-3205156e9699</td>
        <td>Other scheme</td>
        <td>1a54b431-2e4f-452d-9cae-9cee66c9a892</td>
        <td>book</td>
        <td>ef3e6dbc-4b08-56fc-99c9-cf22df8ab042</td>
        <td>808 Him</td>
        <td>896ab6e0-5a77-3129-ae6c-ae31eb3cf197</td>
        <td>Himlische Lieder Johann Rist; Johann Schop</td>
        <td>2b94c631-fca9-4892-a730-03ee529ffe27</td>
        <td>Can circulate</td>
        <td>fcd64ce1-6995-48f0-840e-89ffa2288371</td>
        <td>Main Library</td>
        <td>Available</td>
    </tr>
</table>



### Suppose that we wanted to look at collection use, for example, calculating the proportion of the collection that have circulated.  In general we could do this by dividing the number of distinct items in loans (circulation_loans) by the total number of items (inventory_items).


```python
%%sql

SELECT round( (
           (SELECT count(DISTINCT item_id)::float FROM circulation_loans) /
           (SELECT count(*)::float FROM inventory_items)
       )::numeric, 2)
       AS loan_quotient;
```

     * postgresql://ldp:***@glintcore.net/folio_marc_test
    1 rows affected.





<table>
    <tr>
        <th>loan_quotient</th>
    </tr>
    <tr>
        <td>0.19</td>
    </tr>
</table>



### However, in this case we are interested in only loans in our date range and only print materials.  So we can use our previous results (in item_loan_count_detail) to make this calculation, by dividing the number of items with loan count greater than 0 by the total number of items:


```python
%%sql

/* Books only */
SELECT round( (
           (SELECT count(*)::float FROM local.nassibnassar_item_loan_count_detail WHERE loan_count > 0) /
           (SELECT count(*)::float FROM local.nassibnassar_item_loan_count_detail)
       )::numeric, 2)
       AS loan_quotient;
```

     * postgresql://ldp:***@glintcore.net/folio_marc_test
    1 rows affected.





<table>
    <tr>
        <th>loan_quotient</th>
    </tr>
    <tr>
        <td>0.19</td>
    </tr>
</table>


