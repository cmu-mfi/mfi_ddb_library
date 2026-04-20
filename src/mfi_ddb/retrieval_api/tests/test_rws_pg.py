def test_create_user_query(db_session):
    # Arrange
    db_session.execute("INSERT INTO users (name) VALUES (%s) RETURNING id;", ("Alice",))
    user_id = db_session.fetchone()[0]
    
    # Act
    db_session.execute("SELECT name FROM users WHERE id = %s;", (user_id,))
    result = db_session.fetchone()
    
    # Assert
    assert result[0] == "Alice"