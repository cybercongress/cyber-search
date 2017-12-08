INSERT INTO bitcoin.tx_preview_by_address JSON
'{
    "address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
    "fee": 0,
    "block_number": 0,
    "block_time": "1231006505",
    "hash": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
    "ins": [],
    "outs": [{
        "addresses": ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
        "amount": "50"
    }]
}';


INSERT INTO bitcoin.tx_preview_by_block JSON
'{
    "block_number": 0,
    "index": 0,
    "fee": 0,
    "hash": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
    "ins": [],
     "outs": [{
        "addresses": ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
        "amount": "50"
     }]
}';

INSERT INTO bitcoin.tx JSON
'{
    "hash": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
    "block_number": 0,
    "block_hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
    "block_time": "1231006505",
    "size": 0,
    "coinbase": "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73",
    "fee": "0",
    "total_input": "0",
    "total_output": "50",
    "ins": [],
     "outs": [{
        "addresses": ["1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"],
        "amount": "50"
     }]
}';

INSERT INTO bitcoin.address JSON
'{
    "id": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
    "confirmed_balance": "50",
    "confirmed_total_received": 50,
    "confirmed_tx_number": 1
}';